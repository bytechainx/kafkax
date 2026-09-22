//! [`KafkaPool`] 的 topic 管理面：幂等创建 / 删除与请求形状校验。
//!
//! 从门面 `connection.rs` 下沉（`MR-STRUCT-007` 腾余量）。三个自由函数只被本模块的
//! `ensure_topic` / `delete_topic` 调用，**保持私有**；`is_topic_already_exists_error` /
//! `is_topic_missing_error` 是**纯文本分类**函数，门面内联测试不直接驱动它们。

use super::*;

impl KafkaPool {
    /// 幂等创建 topic（已存在视为成功）。
    ///
    /// # Errors
    ///
    /// topic/分区/副本数非法、连接池已关闭、超时或 broker 拒绝。
    pub async fn ensure_topic(
        &self,
        topic: &str,
        partitions: i32,
        replication: i16,
    ) -> KafkaResult<()> {
        validate_topic_request(topic, partitions, replication)?;
        let _operation = self.start_operation()?;
        let client = self.client()?;
        let mut shutdown = self.shutdown_receiver();
        let controller = client
            .controller_client()
            .map_err(|error| map_kafka_error("kafkax controller", error))?;
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(KafkaError::Closed("create_topic 因连接池关闭而取消".into()));
            }
            result = tokio::time::timeout(
                self.inner.config.operation_timeout,
                controller.create_topic(topic, partitions, replication, 5_000),
            ) => result,
        };
        match result {
            Err(_) => Err(KafkaError::Timeout("create_topic 超时".into())),
            Ok(Ok(())) => {
                self.record_topic_ensured();
                Ok(())
            }
            Ok(Err(error)) => {
                if is_topic_already_exists_error(&error.to_string()) {
                    self.record_topic_ensured();
                    Ok(())
                } else {
                    Err(map_kafka_error("kafkax create_topic", error))
                }
            }
        }
    }

    /// 删除 topic（不存在视为成功）。
    ///
    /// # Errors
    ///
    /// topic 为空、连接池已关闭、超时或 broker 拒绝。
    pub async fn delete_topic(&self, topic: &str) -> KafkaResult<()> {
        if topic.trim().is_empty() {
            return Err(KafkaError::Config("topic 不能为空".into()));
        }
        let _operation = self.start_operation()?;
        let client = self.client()?;
        let mut shutdown = self.shutdown_receiver();
        let controller = client
            .controller_client()
            .map_err(|error| map_kafka_error("kafkax controller", error))?;
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => {
                return Err(KafkaError::Closed("delete_topic 因连接池关闭而取消".into()));
            }
            result = tokio::time::timeout(
                self.inner.config.operation_timeout,
                controller.delete_topic(topic, 5_000),
            ) => result,
        };
        match result {
            Err(_) => Err(KafkaError::Timeout("delete_topic 超时".into())),
            Ok(Ok(())) => {
                self.record_topic_deleted();
                Ok(())
            }
            Ok(Err(error)) => {
                if is_topic_missing_error(&error.to_string()) {
                    self.record_topic_deleted();
                    Ok(())
                } else {
                    Err(map_kafka_error("kafkax delete_topic", error))
                }
            }
        }
    }
}

/// 校验 topic 创建请求的形状。
pub(super) fn validate_topic_request(
    topic: &str,
    partitions: i32,
    replication: i16,
) -> KafkaResult<()> {
    if topic.trim().is_empty() {
        return Err(KafkaError::Config("topic 不能为空".into()));
    }
    if partitions <= 0 {
        return Err(KafkaError::Config("partitions 必须大于零".into()));
    }
    if replication <= 0 {
        return Err(KafkaError::Config("replication 必须大于零".into()));
    }
    Ok(())
}

/// `create_topic` 的错误文本是否表示「topic 已存在」（幂等语义，非失败）。
///
/// `rskafka` 未对该场景暴露结构化错误类型，只能按驱动文本分类；因此本函数独立、可离线单测。
pub(super) fn is_topic_already_exists_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("exist") || text.contains("already") || text.contains("topic_already")
}

/// 删除时 topic 已不存在视为幂等成功。
pub(super) fn is_topic_missing_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("unknown_topic")
        || text.contains("unknown topic")
        || text.contains("does not exist")
        || text.contains("not_exist")
        || text.contains("unknowntopic")
}
