//! [`KafkaPool`]：基于 `rskafka` 的共享客户端、producer/consumer 工厂与生命周期。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};

use crate::config::KafkaConfig;
use crate::consumer::{ConsumerConfig, KafkaConsumer};
use crate::error::{KafkaError, KafkaResult};
use crate::error_map::map_kafka_error;
use crate::lifecycle::{wait_for_shutdown, Lifecycle, OperationGuard};
use crate::producer::KafkaProducer;

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

impl KafkaPool {
    /// 按配置建立连接（含 TLS / SASL 协商），受 `connect_timeout` 约束。
    ///
    /// # Errors
    ///
    /// 配置非法、超时或 broker 不可达。
    pub async fn connect(config: KafkaConfig) -> KafkaResult<Self> {
        config.validate()?;
        let connect_timeout = config.connect_timeout;
        tokio::time::timeout(connect_timeout, Self::connect_inner(config))
            .await
            .map_err(|_| KafkaError::Timeout("kafkax connect 超时".into()))?
    }

    /// 从 `FOUNDATIONX_KAFKAX_*` 环境变量加载配置并连接。
    ///
    /// # Errors
    ///
    /// 与 [`KafkaPool::connect`] 相同。
    pub async fn connect_from_env() -> KafkaResult<Self> {
        Self::connect(KafkaConfig::from_env()?).await
    }

    /// 同步构造：仅校验配置，**不建立网络连接**。
    ///
    /// 该池可用于读取配置/统计与生命周期管理；任何需要 broker 的操作
    /// （[`Self::ping`]、[`Self::health_check`]、[`Self::producer`] 的发布等）都会返回
    /// [`KafkaError::Connection`]。需要真实 I/O 时请用 [`Self::connect`]。
    ///
    /// # Errors
    ///
    /// 配置非法时返回 [`KafkaError::Config`]。
    pub fn new(config: KafkaConfig) -> KafkaResult<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                client: None,
                published: AtomicU64::new(0),
                publish_failed: AtomicU64::new(0),
                publish_timeouts: AtomicU64::new(0),
                publish_cancelled: AtomicU64::new(0),
                topics_ensured: AtomicU64::new(0),
                topics_deleted: AtomicU64::new(0),
                lifecycle: Lifecycle::new(),
            }),
        })
    }

    async fn connect_inner(config: KafkaConfig) -> KafkaResult<Self> {
        let brokers: Vec<String> = config
            .brokers
            .split(',')
            .map(|broker| broker.trim().to_string())
            .filter(|broker| !broker.is_empty())
            .collect();
        let mut builder = ClientBuilder::new(brokers).client_id(config.client_id.clone());
        if config.tls {
            builder = builder.tls_config(build_tls_config(config.tls_ca_file.clone()).await?);
        }
        if let Some((username, password)) = config.sasl_credentials() {
            builder = builder.sasl_config(SaslConfig::Plain(Credentials::new(
                username.to_owned(),
                password.to_owned(),
            )));
        } else if config.sasl_mechanism.is_some() {
            return Err(KafkaError::Config(
                "SASL 机制已设置但缺少 username/password".into(),
            ));
        }
        let client = builder
            .build()
            .await
            .map_err(|error| map_kafka_error("kafkax connect", error))?;
        tracing::debug!(
            protocol = config.security_protocol(),
            client_id = %config.client_id,
            "kafkax 连接已建立"
        );
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                client: Some(client),
                published: AtomicU64::new(0),
                publish_failed: AtomicU64::new(0),
                publish_timeouts: AtomicU64::new(0),
                publish_cancelled: AtomicU64::new(0),
                topics_ensured: AtomicU64::new(0),
                topics_deleted: AtomicU64::new(0),
                lifecycle: Lifecycle::new(),
            }),
        })
    }

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

    /// 共享 producer 句柄。
    #[must_use]
    pub fn producer(&self) -> KafkaProducer {
        KafkaProducer { pool: self.clone() }
    }

    /// 建立分区消费者。
    ///
    /// # Errors
    ///
    /// 连接池已关闭、消费配置非法或分区客户端建立失败。
    pub async fn consumer(&self, config: ConsumerConfig) -> KafkaResult<KafkaConsumer> {
        self.ensure_open()?;
        KafkaConsumer::connect(self.clone(), config).await
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

/// 构建 rustls 客户端配置（公共根证书 + 可选自定义 PEM CA）。
async fn build_tls_config(ca_file: Option<PathBuf>) -> KafkaResult<Arc<rustls::ClientConfig>> {
    tokio::task::spawn_blocking(move || {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = ca_file {
            let metadata = std::fs::metadata(&path)
                .map_err(|error| KafkaError::Config(format!("无法检查 TLS CA 文件: {error}")))?;
            if !metadata.is_file() {
                return Err(KafkaError::Config("TLS CA 路径必须是普通文件".into()));
            }
            if metadata.len() > 1024 * 1024 {
                return Err(KafkaError::Config("TLS CA 文件不得超过 1 MiB".into()));
            }
            let file = std::fs::File::open(&path)
                .map_err(|error| KafkaError::Config(format!("无法读取 TLS CA 文件: {error}")))?;
            let mut reader = std::io::BufReader::new(file);
            let certificates = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| KafkaError::Config(format!("TLS CA PEM 解析失败: {error}")))?;
            if certificates.is_empty() {
                return Err(KafkaError::Config("TLS CA 文件中没有证书".into()));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|error| KafkaError::Config(format!("TLS CA 证书无效: {error}")))?;
            }
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(tls))
    })
    .await
    .map_err(|error| KafkaError::Connection(format!("TLS 配置任务失败: {error}")))?
}

/// 校验 topic 创建请求的形状。
fn validate_topic_request(topic: &str, partitions: i32, replication: i16) -> KafkaResult<()> {
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
fn is_topic_already_exists_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("exist") || text.contains("already") || text.contains("topic_already")
}

/// 删除时 topic 已不存在视为幂等成功。
fn is_topic_missing_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("unknown_topic")
        || text.contains("unknown topic")
        || text.contains("does not exist")
        || text.contains("not_exist")
        || text.contains("unknowntopic")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KafkaConfigBuilder;

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
