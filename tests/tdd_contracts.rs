#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）：逐公开入口先红后绿。
//!
//! 入口集合 = `specs/002-public-api-compliance-and-test-tiers/contracts/public-api-contract.md`
//! 的 kafkax 节。每个入口在变异副本上观测红、在原树观测绿；实际执行的变异与红摘要见 PR 描述。
//!
//! // TDD-PROBE: KafkaConfig::from_env | 变异：apply_env_overlay 忽略 ENV_BROKERS 覆盖 | 红=from_env_overlays_brokers_and_sasl | 绿=from_env_overlays_brokers_and_sasl
//! // TDD-PROBE: KafkaConfig::from_toml | 变异：reject_secret_keys_in_toml 不再拒绝 sasl_password | 红=from_toml_parses_fields_and_rejects_secrets | 绿=from_toml_parses_fields_and_rejects_secrets
//! // TDD-PROBE: KafkaConfig::validate | 变异：去掉「配置 CA 必须启用 TLS」这条 guard | 红=validate_fail_closed_matrix | 绿=validate_fail_closed_matrix
//! // TDD-PROBE: KafkaPool::connect | 变异：connect 不校验配置（跳过 validate） | 红=connect_refused_maps_to_retryable_error | 绿=connect_refused_maps_to_retryable_error
//! // TDD-PROBE: KafkaPool::producer | 变异：producer() 返回未挂载池的句柄 | 红=producer_handle_is_wired_to_pool | 绿=producer_handle_is_wired_to_pool
//! // TDD-PROBE: KafkaPool::consumer | 变异：consumer 在 broker I/O 前不做形状校验 | 红=consumer_requires_connection_and_validates_shape | 绿=consumer_requires_connection_and_validates_shape
//! // TDD-PROBE: KafkaPool::ensure_topic | 变异：partitions 边界改为 <= 1 判定 | 红=ensure_topic_validates_shape_before_io | 绿=ensure_topic_validates_shape_before_io
//! // TDD-PROBE: KafkaPool::ping | 变异：未连接池的 ping 返回 Ok | 红=ping_requires_connection | 绿=ping_requires_connection
//! // TDD-PROBE: KafkaPool::health_check | 变异：health_check 把未连接折叠成 ready=true | 红=health_check_is_strict_and_health_is_tolerant | 绿=health_check_is_strict_and_health_is_tolerant
//! // TDD-PROBE: KafkaProducer::publish | 变异：关闭后的 publish 不再返回 Closed | 红=publish_after_close_reports_closed | 绿=publish_after_close_reports_closed
//! // TDD-PROBE: AtLeastOnceConsumer | 变异：commit 写入 offset 而非 offset+1 | 红=at_least_once_commits_next_offset_and_rejects_bad_shape | 绿=at_least_once_commits_next_offset_and_rejects_bad_shape
//! // TDD-PROBE: KafkaError::is_retryable | 变异：把 Timeout 移到不可重试臂（重试分类反转） | 红=is_retryable_classification_is_exhaustive | 绿=is_retryable_classification_is_exhaustive

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafkax::{
    resolve_start_offset, AtLeastOnceConsumer, ConsumerConfig, KafkaConfig, KafkaError, KafkaPool,
    MemoryOffsetStore, OffsetCommitStore, PublishRecord, ENV_BROKERS, ENV_CLIENT_ID,
    ENV_CONNECT_TIMEOUT_MS, ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS, ENV_SASL_MECHANISM,
    ENV_SASL_PASSWORD, ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};

/// 本文件内会读写的全部环境变量键；集中管理便于用例收尾清理
/// （清理全量键，避免外部已注入的 `FOUNDATIONX_KAFKAX_*` 污染断言）。
const ENV_KEYS: [&str; 10] = [
    ENV_BROKERS,
    ENV_CLIENT_ID,
    ENV_SASL_MECHANISM,
    ENV_SASL_USERNAME,
    ENV_SASL_PASSWORD,
    ENV_TLS,
    ENV_TLS_CA_FILE,
    ENV_CONNECT_TIMEOUT_MS,
    ENV_OPERATION_TIMEOUT_MS,
    ENV_DELIVERY_TIMEOUT_MS,
];

fn clear_env() {
    for key in ENV_KEYS {
        std::env::remove_var(key);
    }
}

/// 入口 `KafkaConfig::from_env`：环境变量覆盖默认值，且凭据在 Debug 中脱敏。
#[test]
fn from_env_overlays_brokers_and_sasl() {
    clear_env();
    std::env::set_var(ENV_BROKERS, "127.0.0.1:9092");
    std::env::set_var(ENV_CLIENT_ID, "kafkax-tdd");
    std::env::set_var(ENV_SASL_MECHANISM, "PLAIN");
    std::env::set_var(ENV_SASL_USERNAME, "tdd-user");
    std::env::set_var(ENV_SASL_PASSWORD, "tdd-secret-value");
    std::env::set_var(ENV_TLS, "false");

    let config = KafkaConfig::from_env().expect("合法环境变量必须可加载");
    clear_env();

    assert_eq!(config.brokers, "127.0.0.1:9092");
    assert_eq!(config.client_id, "kafkax-tdd");
    assert_eq!(config.security_protocol(), "SASL_PLAINTEXT");
    assert!(
        !format!("{config:?}").contains("tdd-secret-value"),
        "密码不得出现在 Debug 输出"
    );
}

/// 入口 `KafkaConfig::from_toml`：解析合法 TOML，并拒绝承载凭据的 TOML。
#[test]
fn from_toml_parses_fields_and_rejects_secrets() {
    let config = KafkaConfig::from_toml(
        "brokers = \"127.0.0.1:9092\"\nclient_id = \"tdd-toml\"\ndelivery_timeout = { secs = 5 }\n",
    )
    .expect("合法 TOML");
    assert_eq!(config.client_id, "tdd-toml");
    assert_eq!(config.delivery_timeout, Duration::from_secs(5));

    let error = KafkaConfig::from_toml(
        "brokers = \"127.0.0.1:9092\"\nsasl_password = \"tdd-secret-value\"\n",
    )
    .expect_err("TOML 承载凭据必须被拒绝");
    assert!(
        !error.to_string().contains("tdd-secret-value"),
        "错误消息不得回显密码"
    );
}

/// 入口 `KafkaConfig::validate`：fail-closed 矩阵（远程明文 / CA 未启 TLS / 零超时）。
#[test]
fn validate_fail_closed_matrix() {
    assert!(
        KafkaConfig::default().validate().is_ok(),
        "默认配置必须合法"
    );

    let mut remote_plain = KafkaConfig::default();
    remote_plain.brokers = "broker.example.com:9092".into();
    assert!(remote_plain.validate().is_err(), "远程明文必须被拒绝");

    let mut ca_without_tls = KafkaConfig::default();
    ca_without_tls.tls_ca_file = Some("/tmp/kafkax-tdd-ca.pem".into());
    assert!(ca_without_tls.validate().is_err(), "CA 文件必须伴随 TLS");

    let mut zero_timeout = KafkaConfig::default();
    zero_timeout.connect_timeout = Duration::ZERO;
    assert!(zero_timeout.validate().is_err(), "零超时必须被拒绝");

    let mut userinfo = KafkaConfig::default();
    userinfo.brokers = "kafka://user:pass@127.0.0.1:9092".into();
    assert!(userinfo.validate().is_err(), "内嵌 userinfo 必须被拒绝");
}

/// 入口 `KafkaPool::connect`：不可达地址在截止时间内失败，且归类为可重试。
#[tokio::test]
async fn connect_refused_maps_to_retryable_error() {
    let config = KafkaConfig::builder()
        .brokers("127.0.0.1:1")
        .client_id("kafkax-tdd-offline")
        .connect_timeout(Duration::from_millis(300))
        .operation_timeout(Duration::from_millis(300))
        .build()
        .expect("回环配置合法");

    let error = tokio::time::timeout(Duration::from_secs(10), KafkaPool::connect(config))
        .await
        .expect("connect 必须受内部截止时间约束")
        .expect_err("端口 1 必然拒绝连接");
    assert!(error.is_retryable(), "连接失败应可重试: {error}");
}

/// 入口 `KafkaPool::producer`：句柄确实挂在连接池上（未连接即 fail-closed）。
#[tokio::test]
async fn producer_handle_is_wired_to_pool() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    let producer = pool.producer();

    let error = producer
        .publish(PublishRecord::payload(
            "tdd-topic",
            0,
            Bytes::from_static(b"x"),
        ))
        .await
        .expect_err("未连接的池不得发布成功");
    assert!(matches!(error, KafkaError::Connection(_)), "{error}");

    // 形状非法在接触 broker 之前就失败。
    let error = producer
        .publish(PublishRecord::payload("  ", 0, Bytes::new()))
        .await
        .expect_err("空 topic 必须被拒绝");
    assert!(matches!(error, KafkaError::Config(_)), "{error}");
}

/// 入口 `KafkaPool::consumer`：先校验形状，再要求已连接。
#[tokio::test]
async fn consumer_requires_connection_and_validates_shape() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");

    assert!(
        matches!(
            pool.consumer(ConsumerConfig::assign("tdd-topic", 0)).await,
            Err(KafkaError::Connection(_))
        ),
        "未连接的池不得建消费者"
    );
    assert!(
        matches!(
            pool.consumer(ConsumerConfig::assign("tdd-topic", -1)).await,
            Err(KafkaError::Config(_))
        ),
        "负分区必须被拒绝"
    );
}

/// 入口 `KafkaPool::ensure_topic`：分区数边界与连接前提。
#[tokio::test]
async fn ensure_topic_validates_shape_before_io() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");

    assert!(
        matches!(
            pool.ensure_topic("tdd-topic", 0, 1).await,
            Err(KafkaError::Config(_))
        ),
        "partitions 必须大于零"
    );
    assert!(
        matches!(
            pool.ensure_topic("tdd-topic", 1, 0).await,
            Err(KafkaError::Config(_))
        ),
        "replication 必须大于零"
    );
    assert!(
        matches!(
            pool.ensure_topic("tdd-topic", 1, 1).await,
            Err(KafkaError::Connection(_))
        ),
        "形状合法但未连接必须返回 Connection"
    );
}

/// 入口 `KafkaPool::ping`：未连接池必须报错，不得伪报成功。
#[tokio::test]
async fn ping_requires_connection() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    assert!(
        matches!(pool.ping().await, Err(KafkaError::Connection(_))),
        "未连接的池 ping 必须失败"
    );
}

/// 入口 `KafkaPool::health_check`：严格版报错、宽容版给出 ready=false 与分类摘要。
#[tokio::test]
async fn health_check_is_strict_and_health_is_tolerant() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");

    assert!(
        matches!(pool.health_check().await, Err(KafkaError::Connection(_))),
        "health_check 在未连接时必须报错"
    );
    let health = pool.health().await.expect("health 对未连接不报错");
    assert!(!health.ready, "未连接不得标记 ready");
    assert!(
        health.detail.contains("connection"),
        "detail 应含分类摘要: {}",
        health.detail
    );
}

/// 入口 `KafkaProducer::publish`：关闭后的池立即取消发布并计入取消。
#[tokio::test]
async fn publish_after_close_reports_closed() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    pool.close(Duration::from_millis(200)).await.expect("关闭");

    let error = pool
        .producer()
        .publish(PublishRecord::payload("tdd-topic", 0, Bytes::new()))
        .await
        .expect_err("已关闭的池不得发布");
    assert!(matches!(error, KafkaError::Closed(_)), "{error}");
    assert!(!error.is_retryable(), "Closed 不可重试");
    assert!(pool.stats().publish_cancelled >= 1, "应计入取消计数");
}

/// 入口 `AtLeastOnceConsumer`：位点语义（next-to-read = offset + 1）与形状校验。
#[tokio::test]
async fn at_least_once_commits_next_offset_and_rejects_bad_shape() {
    let store = MemoryOffsetStore::new().shared();
    store
        .commit("tdd-topic", 0, 7)
        .await
        .expect("提交已处理 offset");
    assert_eq!(
        store.committed("tdd-topic", 0).await.expect("读取位点"),
        Some(8),
        "提交 offset=7 后下一次应读 8"
    );
    assert_eq!(
        resolve_start_offset(store.as_ref(), "tdd-topic", 0)
            .await
            .expect("解析起点"),
        Some(8)
    );

    let shared: Arc<dyn OffsetCommitStore> = store;
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    assert!(
        matches!(
            AtLeastOnceConsumer::connect(
                pool.clone(),
                ConsumerConfig::assign("", 0),
                Arc::clone(&shared)
            )
            .await,
            Err(KafkaError::Config(_))
        ),
        "空 topic 必须在建连前被拒绝"
    );
    assert!(
        matches!(
            AtLeastOnceConsumer::connect(pool, ConsumerConfig::assign("tdd-topic", 0), shared)
                .await,
            Err(KafkaError::Connection(_))
        ),
        "形状合法但未连接必须返回 Connection"
    );
}

/// 入口 `KafkaError::is_retryable`：三可重试 / 六不可重试的分类是穷尽的。
#[test]
fn is_retryable_classification_is_exhaustive() {
    let retryable = [
        KafkaError::Connection("x".into()),
        KafkaError::Transient("x".into()),
        KafkaError::Timeout("x".into()),
    ];
    for error in retryable {
        assert!(error.is_retryable(), "{} 应可重试", error.kind());
    }

    let permanent = [
        KafkaError::Config("x".into()),
        KafkaError::Backend("x".into()),
        KafkaError::Serialization("x".into()),
        KafkaError::Io(std::io::Error::other("x")),
        KafkaError::Unsupported("x".into()),
        KafkaError::Closed("x".into()),
    ];
    for error in permanent {
        assert!(!error.is_retryable(), "{} 不应重试", error.kind());
    }
}
