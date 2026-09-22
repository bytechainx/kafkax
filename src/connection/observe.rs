//! [`KafkaPool`] 的观测面：配置 / 客户端取值与健康检查、计数快照。
//!
//! 从门面 `connection.rs` 下沉（`MR-STRUCT-007` 腾余量）。全部方法原本就是 `pub`，
//! **无可见性调整**。`client()` 是 `pub`，故 `runtime.rs` 也能调用。

use super::*;

impl KafkaPool {
    /// 当前配置。
    #[must_use]
    pub fn config(&self) -> &KafkaConfig {
        &self.inner.config
    }

    /// 底层 `rskafka` 客户端；[`KafkaPool::new`] 构造的未连接池会返回
    /// [`KafkaError::Connection`]。
    ///
    /// # Errors
    ///
    /// 连接池未经 [`KafkaPool::connect`] 建立时失败。
    pub fn client(&self) -> KafkaResult<&Client> {
        self.inner.client.as_ref().ok_or_else(|| {
            KafkaError::Connection(
                "连接未建立：请使用 KafkaPool::connect 或 connect_from_env".into(),
            )
        })
    }

    /// 判断集群可达：拉取一次 metadata（`list_topics`）。
    ///
    /// # Errors
    ///
    /// 未连接、连接池已关闭、超时或 broker 不可达。
    pub async fn ping(&self) -> KafkaResult<()> {
        self.health_check().await.map(|_| ())
    }

    /// 健康检查（结构化，broker 故障不报错）。
    ///
    /// 返回 `ready = false` 并附分类摘要；只有连接池已关闭时返回
    /// [`KafkaError::Closed`]，未建立连接时返回 `ready = false`。
    ///
    /// # Errors
    ///
    /// 连接池已关闭。
    pub async fn health(&self) -> KafkaResult<KafkaHealth> {
        match self.health_check().await {
            Ok(health) => Ok(health),
            Err(error) if matches!(error, KafkaError::Closed(_)) => Err(error),
            Err(error) => Ok(KafkaHealth {
                ready: false,
                detail: format!("{}: {error}", error.kind()),
            }),
        }
    }

    /// 健康检查（严格）：不可达即返回错误。
    ///
    /// # Errors
    ///
    /// 未连接、连接池已关闭、超时或 broker 不可达。
    pub async fn health_check(&self) -> KafkaResult<KafkaHealth> {
        let _operation = self.start_operation()?;
        let client = self.client()?;
        let mut shutdown = self.shutdown_receiver();
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(KafkaError::Closed("list_topics 因连接池关闭而取消".into()));
            }
            result = tokio::time::timeout(self.inner.config.operation_timeout, client.list_topics()) => result,
        };
        match result {
            Err(_) => Err(KafkaError::Timeout("list_topics 超时".into())),
            Ok(Err(error)) => Err(map_kafka_error("kafkax list_topics", error)),
            Ok(Ok(topics)) => Ok(KafkaHealth {
                ready: true,
                detail: format!("topics={}", topics.len()),
            }),
        }
    }

    /// 累计统计。
    #[must_use]
    pub fn stats(&self) -> KafkaPoolStats {
        KafkaPoolStats {
            published: self.inner.published.load(Ordering::Relaxed),
            publish_failed: self.inner.publish_failed.load(Ordering::Relaxed),
            publish_timeouts: self.inner.publish_timeouts.load(Ordering::Relaxed),
            publish_cancelled: self.inner.publish_cancelled.load(Ordering::Relaxed),
            topics_ensured: self.inner.topics_ensured.load(Ordering::Relaxed),
            topics_deleted: self.inner.topics_deleted.load(Ordering::Relaxed),
            closed: self.inner.lifecycle.is_closed(),
        }
    }
}
