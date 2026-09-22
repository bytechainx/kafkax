//! [`KafkaPool`] 的运行时面：关闭 / 关闭态查询，以及供 `producer` / `consumer` /
//! `offset` 使用的 `pub(crate)` 访问器（在途守卫、关闭信号、计数与分区客户端）。
//!
//! 从门面 `connection.rs` 下沉（`MR-STRUCT-007` 腾余量）。搬走的项要么是 `pub`
//! （`close` / `is_closed`），要么是 `pub(crate)`，**无可见性调整**。

use super::*;

impl KafkaPool {
    /// 关闭连接池：拒绝新请求、取消在途 broker I/O 与后台消费任务，并在 `deadline`
    /// 内等待在途操作释放。
    ///
    /// deadline 到期后连接池仍保持关闭，调用方可再次调用继续等待。
    ///
    /// # Errors
    ///
    /// 等待在途操作超过 `deadline` 时返回 [`KafkaError::Timeout`]。
    pub async fn close(&self, deadline: Duration) -> KafkaResult<()> {
        self.inner.lifecycle.close(deadline).await
    }

    /// 连接池是否已关闭。
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.lifecycle.is_closed()
    }

    /// 校验连接池仍可接受操作。
    pub(crate) fn ensure_open(&self) -> KafkaResult<()> {
        self.inner.lifecycle.ensure_open()
    }

    /// 注册在途操作守卫。
    pub(crate) fn start_operation(&self) -> KafkaResult<OperationGuard> {
        self.inner.lifecycle.start_operation()
    }

    /// 订阅关闭信号。
    pub(crate) fn shutdown_receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner.lifecycle.subscribe_shutdown()
    }

    /// 记录一次成功的 publish。
    pub(crate) fn record_publish_ok(&self) {
        self.inner.published.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次失败的 publish。
    pub(crate) fn record_publish_err(&self) {
        self.inner.publish_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次投递超时（同时计入失败）。
    pub(crate) fn record_publish_timeout(&self) {
        self.inner.publish_timeouts.fetch_add(1, Ordering::Relaxed);
        self.inner.publish_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次因关闭被取消的 produce（同时计入失败）。
    pub(crate) fn record_publish_cancelled(&self) {
        self.inner.publish_cancelled.fetch_add(1, Ordering::Relaxed);
        self.inner.publish_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次 topic 幂等创建。
    pub(crate) fn record_topic_ensured(&self) {
        self.inner.topics_ensured.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次 topic 幂等删除。
    pub(crate) fn record_topic_deleted(&self) {
        self.inner.topics_deleted.fetch_add(1, Ordering::Relaxed);
    }

    /// 取得分区客户端（受 `operation_timeout` 与关闭信号约束）。
    pub(crate) async fn partition_client(
        &self,
        topic: &str,
        partition: i32,
    ) -> KafkaResult<rskafka::client::partition::PartitionClient> {
        if topic.trim().is_empty() {
            return Err(KafkaError::Config("topic 不能为空".into()));
        }
        if partition < 0 {
            return Err(KafkaError::Config("partition 不能为负".into()));
        }
        let _operation = self.start_operation()?;
        let client = self.client()?;
        let mut shutdown = self.shutdown_receiver();
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                Err(KafkaError::Closed("partition_client 因连接池关闭而取消".into()))
            }
            result = tokio::time::timeout(
                self.inner.config.operation_timeout,
                client.partition_client(topic, partition, UnknownTopicHandling::Retry),
            ) => {
                match result {
                    Err(_) => Err(KafkaError::Timeout("partition_client 超时".into())),
                    Ok(Err(error)) => Err(map_kafka_error("kafkax partition_client", error)),
                    Ok(Ok(client)) => Ok(client),
                }
            }
        }
    }

    /// produce 压缩方式（当前固定不压缩，与 broker 默认行为一致）。
    pub(crate) fn compression() -> Compression {
        Compression::NoCompression
    }
}
