//! Kafka 消费者：按 **显式分区 + 起始 offset** 流式读取。
//!
//! 这里**没有** consumer group / rebalance：调用方用 [`ConsumerConfig::assign`] 指定分区，
//! 用 [`ConsumerConfig::with_start_offset`] 指定起始位点（通常来自
//! [`OffsetCommitStore::committed`](crate::OffsetCommitStore::committed)）。

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::PartitionClient;
use tokio::sync::mpsc;

use crate::error::{KafkaError, KafkaResult};
use crate::error_map::map_kafka_error;
use crate::lifecycle::{send_or_shutdown, wait_for_shutdown};
use crate::message::KafkaMessage;
use crate::pool::KafkaPool;

/// 消费缓冲队列容量（固定值；慢消费者通过等待施加背压）。
const CONSUMER_BUFFER_CAPACITY: usize = 64;

/// 消费配置。
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// topic。
    pub topic: String,
    /// 分区（本库按分区消费，不参与 group 分配）。
    pub partition: i32,
    /// 未指定 `start_offset` 时：`true` 从最早开始，`false` 从最新开始。
    pub from_beginning: bool,
    /// 显式起始 offset（等价 `StartOffset::At`），优先于 `from_beginning`。
    pub start_offset: Option<i64>,
}

impl ConsumerConfig {
    /// 订阅 topic 的分区 0，从最早开始。
    pub fn subscribe(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: 0,
            from_beginning: true,
            start_offset: None,
        }
    }

    /// 显式指定 topic 与分区，从最早开始。
    pub fn assign(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            from_beginning: true,
            start_offset: None,
        }
    }

    /// 指定显式起始 offset（覆盖 `from_beginning`）。
    #[must_use]
    pub fn with_start_offset(mut self, offset: i64) -> Self {
        self.start_offset = Some(offset);
        self.from_beginning = false;
        self
    }

    /// 解析为 `rskafka` 的 [`StartOffset`]。
    #[must_use]
    pub fn resolve_start_offset(&self) -> StartOffset {
        if let Some(offset) = self.start_offset {
            StartOffset::At(offset)
        } else if self.from_beginning {
            StartOffset::Earliest
        } else {
            StartOffset::Latest
        }
    }
}

/// 分区消费会话；`Drop` 会终止后台拉取任务。
pub struct KafkaConsumer {
    rx: mpsc::Receiver<KafkaResult<KafkaMessage>>,
    pool: KafkaPool,
    task: tokio::task::JoinHandle<()>,
}

impl KafkaConsumer {
    /// 建立分区消费会话（仅供 [`KafkaPool::consumer`] 调用）。
    pub(crate) async fn connect(pool: KafkaPool, config: ConsumerConfig) -> KafkaResult<Self> {
        validate_consumer_config(&config)?;
        let client: Arc<PartitionClient> = Arc::new(
            pool.partition_client(&config.topic, config.partition)
                .await?,
        );
        let start = config.resolve_start_offset();
        let mut stream = StreamConsumerBuilder::new(client, start).build();
        let (tx, rx) = mpsc::channel(CONSUMER_BUFFER_CAPACITY);
        let topic = config.topic.clone();
        let partition = config.partition;
        let operation = pool.start_operation()?;
        let mut shutdown = pool.shutdown_receiver();
        let task = tokio::spawn(async move {
            let _operation = operation;
            loop {
                let item = tokio::select! {
                    biased;
                    () = wait_for_shutdown(&mut shutdown) => break,
                    item = stream.next() => item,
                };
                let Some(item) = item else {
                    break;
                };
                let output = match item {
                    Ok((record_offset, _high_watermark)) => {
                        let record = record_offset.record;
                        let headers = record
                            .headers
                            .into_iter()
                            .map(|(k, v)| (k, Bytes::from(v)))
                            .collect();
                        Ok(KafkaMessage {
                            topic: topic.clone(),
                            partition,
                            offset: record_offset.offset,
                            payload: Bytes::from(record.value.unwrap_or_default()),
                            key: record.key.map(Bytes::from),
                            headers,
                            timestamp: Some(record.timestamp),
                        })
                    }
                    Err(error) => Err(map_kafka_error("kafkax fetch", error)),
                };
                let terminal_error = output.is_err();
                if !send_or_shutdown(&tx, output, &mut shutdown).await || terminal_error {
                    break;
                }
            }
        });
        Ok(Self { rx, pool, task })
    }

    /// 取下一条消息；流结束或连接池关闭时返回 `None`。
    pub async fn recv(&mut self) -> Option<KafkaResult<KafkaMessage>> {
        if let Err(error) = self.pool.ensure_open() {
            return Some(Err(error));
        }
        self.rx.recv().await
    }

    /// 带超时接收。
    ///
    /// # Errors
    ///
    /// 超时返回 [`KafkaError::Timeout`]；拉取失败返回底层映射错误。
    pub async fn recv_timeout(&mut self, timeout: Duration) -> KafkaResult<Option<KafkaMessage>> {
        self.pool.ensure_open()?;
        match tokio::time::timeout(timeout, self.rx.recv()).await {
            Ok(Some(Ok(message))) => Ok(Some(message)),
            Ok(Some(Err(error))) => Err(error),
            Ok(None) => Ok(None),
            Err(_) => Err(KafkaError::Timeout("consumer recv 超时".into())),
        }
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// 在发起 broker I/O 前校验消费配置形状（fail-closed）。
pub(crate) fn validate_consumer_config(config: &ConsumerConfig) -> KafkaResult<()> {
    if config.topic.trim().is_empty() {
        return Err(KafkaError::Config("consumer topic 不能为空".into()));
    }
    if config.partition < 0 {
        return Err(KafkaError::Config("consumer partition 不能为负".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_offset_matrix() {
        let mut config = ConsumerConfig::subscribe("t");
        assert!(matches!(
            config.resolve_start_offset(),
            StartOffset::Earliest
        ));
        config.from_beginning = false;
        assert!(matches!(config.resolve_start_offset(), StartOffset::Latest));
        config.start_offset = Some(12);
        assert!(matches!(config.resolve_start_offset(), StartOffset::At(12)));

        let assigned = ConsumerConfig::assign("t", 1).with_start_offset(5);
        assert_eq!(assigned.partition, 1);
        assert!(!assigned.from_beginning);
        assert!(matches!(
            assigned.resolve_start_offset(),
            StartOffset::At(5)
        ));
    }

    #[test]
    fn consumer_buffer_is_intentionally_bounded() {
        assert_eq!(CONSUMER_BUFFER_CAPACITY, 64);
    }

    #[test]
    fn validation_rejects_bad_shape_before_broker_io() {
        assert!(validate_consumer_config(&ConsumerConfig::subscribe("  ")).is_err());
        assert!(validate_consumer_config(&ConsumerConfig::assign("t", -1)).is_err());
        validate_consumer_config(&ConsumerConfig::assign("orders", 0)).expect("形状合法");
    }
}
