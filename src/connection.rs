//! 低层：基于 `rskafka` 的共享客户端、池状态、生命周期与 topic/健康操作。
//!
//! 本模块定义 [`KafkaPool`] 及其不涉及 `consumer` / `producer` 类型的全部方法。
//! 需要构造 producer / consumer 的工厂方法（`producer` / `consumer`）位于
//! [`crate::pool`]，使依赖方向保持单向：
//! `pool` → `consumer` / `producer` → `connection`，三者之间无模块环（`MR-DEP-001`）。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};

use crate::config::KafkaConfig;
use crate::error::{KafkaError, KafkaResult};
use crate::error_map::map_kafka_error;
use crate::lifecycle::{wait_for_shutdown, Lifecycle, OperationGuard};

/// 连接池统计（低基数计数，覆盖生产热路径）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KafkaPoolStats {
    /// 成功 publish 次数。
    pub published: u64,
    /// publish 失败次数（含 broker 错误、超时与取消）。
    pub publish_failed: u64,
    /// produce 投递确认超时次数。
    pub publish_timeouts: u64,
    /// produce 因连接池关闭被取消次数。
    pub publish_cancelled: u64,
    /// `ensure_topic` 成功次数（含 already-exists 幂等成功）。
    pub topics_ensured: u64,
    /// `delete_topic` 成功次数（含 missing 幂等成功）。
    pub topics_deleted: u64,
    /// 连接池是否已关闭。
    pub closed: bool,
}

/// 健康检查结果。
#[derive(Debug, Clone)]
pub struct KafkaHealth {
    /// 集群是否可达。
    pub ready: bool,
    /// 可读说明（不包含凭据；失败时为分类摘要）。
    pub detail: String,
}

/// Kafka 连接池（可克隆，内部共享 `Arc`）。
#[derive(Clone, Debug)]
pub struct KafkaPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: KafkaConfig,
    /// [`KafkaPool::new`] 只做校验，故此处可为 `None`；[`KafkaPool::connect`] 必定为 `Some`。
    client: Option<Client>,
    published: AtomicU64,
    publish_failed: AtomicU64,
    publish_timeouts: AtomicU64,
    publish_cancelled: AtomicU64,
    topics_ensured: AtomicU64,
    topics_deleted: AtomicU64,
    lifecycle: Lifecycle,
}

impl std::fmt::Debug for PoolInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolInner")
            .field("config", &self.config)
            .field("connected", &self.client.is_some())
            .finish_non_exhaustive()
    }
}

mod connect;
mod observe;
mod runtime;
mod topic;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KafkaConfigBuilder;

    // 下沉到 `connection/topic.rs` 的纯函数：`pub(super)` 项不会被 `use super::*` 捞到，
    // 故在测试模块内显式导入。
    use super::topic::{
        is_topic_already_exists_error, is_topic_missing_error, validate_topic_request,
    };

    #[tokio::test]
    async fn connect_to_refused_address_fails_within_deadline() {
        let config = KafkaConfigBuilder::new()
            .brokers("127.0.0.1:1")
            .connect_timeout(Duration::from_millis(200))
            .operation_timeout(Duration::from_millis(200))
            .build()
            .expect("测试配置合法");
        let result = tokio::time::timeout(Duration::from_secs(5), KafkaPool::connect(config))
            .await
            .expect("必须在内部截止时间内返回");
        assert!(result.is_err(), "拒绝连接必须失败");
    }

    #[test]
    fn new_pool_is_unconnected_and_reports_connection_error() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        assert!(!pool.stats().closed);
        let error = pool.client().expect_err("未连接");
        assert!(matches!(error, KafkaError::Connection(_)));
    }

    #[tokio::test]
    async fn health_on_unconnected_pool_is_not_ready() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        assert!(pool.ping().await.is_err());
        let health = pool.health().await.expect("未连接不是致命错误");
        assert!(!health.ready);
    }

    #[tokio::test]
    async fn close_marks_stats_closed_and_rejects_new_operations() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        pool.close(Duration::from_millis(200)).await.expect("关闭");
        assert!(pool.stats().closed);
        assert!(pool.is_closed());
        assert!(matches!(
            pool.ensure_open().expect_err("已关闭"),
            KafkaError::Closed(_)
        ));
        assert!(matches!(
            pool.health().await.expect_err("已关闭时 health 报错"),
            KafkaError::Closed(_)
        ));
    }

    #[test]
    fn topic_request_shape_is_validated_before_broker_io() {
        for (topic, partitions, replication) in [("", 1, 1), ("t", 0, 1), ("t", 1, 0)] {
            assert!(validate_topic_request(topic, partitions, replication).is_err());
        }
        validate_topic_request("orders", 3, 1).expect("合法请求");
    }

    #[test]
    fn topic_error_text_classification() {
        for message in [
            "Topic already exists",
            "TOPIC_ALREADY_EXISTS",
            "already present",
        ] {
            assert!(
                is_topic_already_exists_error(message),
                "应识别为已存在: {message}"
            );
        }
        for message in [
            "connection refused",
            "not authorized",
            "invalid replication factor",
        ] {
            assert!(
                !is_topic_already_exists_error(message),
                "不应误判: {message}"
            );
        }
        for message in ["UNKNOWN_TOPIC_OR_PARTITION", "does not exist", "not_exist"] {
            assert!(is_topic_missing_error(message), "应识别为不存在: {message}");
        }
    }

    #[test]
    fn counters_follow_record_paths() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        assert_eq!(pool.stats(), KafkaPoolStats::default());
        pool.record_publish_timeout();
        let stats = pool.stats();
        assert_eq!(
            (
                stats.publish_timeouts,
                stats.publish_cancelled,
                stats.publish_failed
            ),
            (1, 0, 1)
        );
        pool.record_publish_cancelled();
        let stats = pool.stats();
        assert_eq!(
            (
                stats.publish_timeouts,
                stats.publish_cancelled,
                stats.publish_failed
            ),
            (1, 1, 2)
        );
        pool.record_publish_ok();
        pool.record_topic_ensured();
        pool.record_topic_deleted();
        let stats = pool.stats();
        assert_eq!(
            (stats.published, stats.topics_ensured, stats.topics_deleted),
            (1, 1, 1)
        );
    }
}
