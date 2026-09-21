//! At-least-once 消费：显式 `ack` 后才推进位点。
//!
//! ## 语义
//!
//! - 消息交付后进入 pending；在 `ack` 之前不会从其下游再取新消息。
//! - `ack` 把 pending 消息的 offset 写入 [`OffsetCommitStore`]（next-to-read = offset + 1）。
//! - 断线重连时用 `StartOffset::At` 从 store 的 committed next-to-read 重启。
//! - 未 ack 即 drop：store 仍保留上次提交点 → 重连会重投（这正是 at-least-once 的来源）。
//!
//! 该语义由应用层实现：`rskafka` 没有 consumer group，broker 侧不会替你保存位点。

use std::sync::Arc;
use std::time::Duration;

use crate::connection::KafkaPool;
use crate::consumer::{ConsumerConfig, KafkaConsumer};
use crate::error::{KafkaError, KafkaResult};
use crate::message::KafkaMessage;
use crate::offset::OffsetCommitStore;

/// 底层消费源：生产路径为 live consumer；单测可注入 unit 后端（无 broker）。
enum ConsumerBackend {
    Live(KafkaConsumer),
    #[cfg(test)]
    Unit,
}

/// At-least-once 分区消费者（单 owner：一个 `(topic, partition)` 只应有一个实例）。
pub struct AtLeastOnceConsumer {
    inner: ConsumerBackend,
    store: Arc<dyn OffsetCommitStore>,
    topic: String,
    partition: i32,
    pending: Option<KafkaMessage>,
    /// `drop_pending_unacked` 后为 `true`；此后禁止继续 recv/ack。
    terminated: bool,
}

impl AtLeastOnceConsumer {
    /// 连接：若 store 中已有 committed next-to-read，则从该 offset 启动。
    ///
    /// # Errors
    ///
    /// topic 为空、store 读取失败或底层消费者建立失败。
    pub async fn connect(
        pool: KafkaPool,
        mut config: ConsumerConfig,
        store: Arc<dyn OffsetCommitStore>,
    ) -> KafkaResult<Self> {
        if config.topic.trim().is_empty() {
            return Err(KafkaError::Config("at-least-once topic 不能为空".into()));
        }
        if config.partition < 0 {
            return Err(KafkaError::Config(
                "at-least-once partition 不能为负".into(),
            ));
        }
        let topic = config.topic.clone();
        let partition = config.partition;
        if let Some(next) = store.committed(&topic, partition).await? {
            config.start_offset = Some(next);
            config.from_beginning = false;
        }
        let inner = pool.consumer(config).await?;
        Ok(Self {
            inner: ConsumerBackend::Live(inner),
            store,
            topic,
            partition,
            pending: None,
            terminated: false,
        })
    }

    /// 单测用：注入 pending 与状态机（**不构造 broker 连接**）。
    #[cfg(test)]
    pub(crate) fn for_unit_test(
        store: Arc<dyn OffsetCommitStore>,
        topic: impl Into<String>,
        partition: i32,
        pending: Option<KafkaMessage>,
    ) -> Self {
        Self {
            inner: ConsumerBackend::Unit,
            store,
            topic: topic.into(),
            partition,
            pending,
            terminated: false,
        }
    }

    /// topic。
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// 分区。
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// 当前未 ack 的消息（只读）。
    #[must_use]
    pub fn pending(&self) -> Option<&KafkaMessage> {
        self.pending.as_ref()
    }

    /// 取下一条消息。
    ///
    /// 若已有未 ack 的 pending，直接返回该 pending（不向 broker 再取）。
    pub async fn recv(&mut self) -> Option<KafkaResult<KafkaMessage>> {
        if let Err(error) = self.ensure_active() {
            return Some(Err(error));
        }
        if let Some(message) = &self.pending {
            return Some(Ok(message.clone()));
        }
        match &mut self.inner {
            ConsumerBackend::Live(consumer) => match consumer.recv().await {
                Some(Ok(message)) => {
                    self.pending = Some(message.clone());
                    Some(Ok(message))
                }
                Some(Err(error)) => Some(Err(error)),
                None => None,
            },
            #[cfg(test)]
            ConsumerBackend::Unit => None,
        }
    }

    /// 带超时接收（pending 优先，不计时）。
    ///
    /// # Errors
    ///
    /// 会话已终止返回 [`KafkaError::Closed`]；超时返回 [`KafkaError::Timeout`]。
    pub async fn recv_timeout(&mut self, timeout: Duration) -> KafkaResult<Option<KafkaMessage>> {
        self.ensure_active()?;
        if let Some(message) = &self.pending {
            return Ok(Some(message.clone()));
        }
        match &mut self.inner {
            ConsumerBackend::Live(consumer) => match consumer.recv_timeout(timeout).await? {
                Some(message) => {
                    self.pending = Some(message.clone());
                    Ok(Some(message))
                }
                None => Ok(None),
            },
            #[cfg(test)]
            ConsumerBackend::Unit => Ok(None),
        }
    }

    /// 确认 pending 消息：写入 store（next = offset + 1）并清除 pending。
    ///
    /// **仅当 commit 成功才清除 pending**：store I/O 失败时 pending 保留，可重试 `ack`。
    ///
    /// # Errors
    ///
    /// 无 pending 可 ack、会话已终止或 store 写入失败。
    pub async fn ack(&mut self) -> KafkaResult<()> {
        self.ensure_active()?;
        let message = self
            .pending
            .clone()
            .ok_or_else(|| KafkaError::Config("无 pending 消息可 ack".into()))?;
        self.store
            .commit(&message.topic, message.partition, message.offset)
            .await?;
        self.pending = None;
        Ok(())
    }

    /// 当前 store 中 committed 的 next-to-read。
    ///
    /// # Errors
    ///
    /// store 读取失败。
    pub async fn committed(&self) -> KafkaResult<Option<i64>> {
        self.store.committed(&self.topic, self.partition).await
    }

    /// 保留 pending 但不提交（本地失败时使用；下次 `recv` 仍返回同一消息）。
    pub fn nack_keep_pending(&mut self) {
        // 语义上不做任何变更：pending 保留即代表未提交。
    }

    /// 丢弃 pending 且 **不** 提交，并终止会话。
    ///
    /// 终止后 `recv`/`ack` 返回 [`KafkaError::Closed`]，避免在未 ack 的情况下继续消费并越过位点；
    /// 重连后会从 last committed 处重投（at-least-once 的重复窗口）。
    pub fn drop_pending_unacked(&mut self) {
        self.pending = None;
        self.terminated = true;
    }

    /// 会话是否已因 [`Self::drop_pending_unacked`] 终止。
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// 终止检查。
    fn ensure_active(&self) -> KafkaResult<()> {
        if self.terminated {
            Err(KafkaError::Closed(
                "at-least-once 会话已因 drop_pending_unacked 终止；请重连".into(),
            ))
        } else {
            Ok(())
        }
    }
}

/// 纯逻辑：从 store 解析启动 offset（`None` 表示 store 中无记录）。
///
/// # Errors
///
/// store 读取失败。
pub async fn resolve_start_offset(
    store: &dyn OffsetCommitStore,
    topic: &str,
    partition: i32,
) -> KafkaResult<Option<i64>> {
    store.committed(topic, partition).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::MemoryOffsetStore;
    use bytes::Bytes;

    fn sample_message(offset: i64) -> KafkaMessage {
        KafkaMessage {
            topic: "orders".into(),
            partition: 0,
            offset,
            payload: Some(Bytes::from_static(b"a")),
            key: None,
            headers: Default::default(),
            timestamp: None,
        }
    }

    #[tokio::test]
    async fn ack_advances_committed_offset() {
        let store = MemoryOffsetStore::new().shared();
        assert!(resolve_start_offset(store.as_ref(), "t", 0)
            .await
            .expect("读取")
            .is_none());
        let mut consumer = AtLeastOnceConsumer::for_unit_test(
            Arc::clone(&store) as Arc<dyn OffsetCommitStore>,
            "orders",
            0,
            Some(sample_message(7)),
        );
        assert_eq!(consumer.pending().map(|message| message.offset), Some(7));
        consumer.ack().await.expect("ack");
        assert!(consumer.pending().is_none());
        assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(8));
        assert_eq!(
            resolve_start_offset(store.as_ref(), "orders", 0)
                .await
                .expect("解析"),
            Some(8)
        );
    }

    #[tokio::test]
    async fn nack_keeps_pending_and_does_not_commit() {
        let store = MemoryOffsetStore::new().shared();
        let mut consumer = AtLeastOnceConsumer::for_unit_test(
            Arc::clone(&store) as Arc<dyn OffsetCommitStore>,
            "orders",
            0,
            Some(sample_message(9)),
        );
        consumer.nack_keep_pending();
        assert!(!consumer.is_terminated());
        assert_eq!(
            consumer.recv().await.expect("有结果").expect("成功").offset,
            9
        );
        assert!(store.committed("orders", 0).await.expect("读取").is_none());
    }

    #[tokio::test]
    async fn drop_pending_unacked_terminates_session() {
        let store = MemoryOffsetStore::new().shared();
        let mut consumer = AtLeastOnceConsumer::for_unit_test(
            Arc::clone(&store) as Arc<dyn OffsetCommitStore>,
            "orders",
            0,
            Some(sample_message(3)),
        );
        consumer.drop_pending_unacked();
        assert!(consumer.is_terminated());
        assert!(consumer.pending().is_none());
        assert!(store.committed("orders", 0).await.expect("读取").is_none());

        assert!(matches!(
            consumer.recv().await.expect("有结果").expect_err("已终止"),
            KafkaError::Closed(_)
        ));
        assert!(matches!(
            consumer.ack().await.expect_err("已终止"),
            KafkaError::Closed(_)
        ));
        assert!(matches!(
            consumer
                .recv_timeout(Duration::from_millis(10))
                .await
                .expect_err("已终止"),
            KafkaError::Closed(_)
        ));
    }
}
