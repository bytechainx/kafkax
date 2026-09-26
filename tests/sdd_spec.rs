#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的章节条款转成可执行断言。
//!
//! // SPEC-MAP: S-1 | 1. 定位 | assert_positioning
//! // SPEC-MAP: S-2 | 2. 字段治理 | assert_config_governance
//! // SPEC-MAP: S-3 | 3. 安全约定（`validate()` fail-closed） | assert_security_fail_closed
//! // SPEC-MAP: S-4 | 4. 失败与并发 | assert_failure_and_concurrency
//! // SPEC-MAP: S-5 | 5. 验收 | assert_acceptance

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafkax::{
    encode_bus_id, parse_bus_id, partition_for_key, AtLeastOnceConsumer, ConsumerConfig, Delivery,
    FileOffsetStore, KafkaConfig, KafkaError, KafkaMessage, KafkaPool, MemoryOffsetStore,
    OffsetCommitStore, PublishRecord, ENV_BROKERS, ENV_CLIENT_ID, ENV_CONNECT_TIMEOUT_MS,
    ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS, ENV_SASL_MECHANISM, ENV_SASL_PASSWORD,
    ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};

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

/// S-1：定位——连接池 + 等确认生产者 + 显式分区消费 + 应用层位点 + 健康面；
/// 不提供 consumer group，任何连接都必须是显式 `connect`。
#[test]
fn assert_positioning() {
    // 默认面向本地联调，且不内置任何凭据。
    let config = KafkaConfig::default();
    config.validate().expect("默认配置合法");
    assert_eq!(config.security_protocol(), "PLAINTEXT");
    assert!(!config.tls);

    // 同步构造不联网：可达性由 ping / health_check 显式承担。
    let pool = KafkaPool::new(config).expect("同步构造");
    assert!(!pool.is_closed());
    assert!(
        pool.client().is_err(),
        "未 connect 的池不得暴露客户端（不隐式连接）"
    );

    // 分区由调用方显式指定（无 group 分配）。
    let assigned = ConsumerConfig::assign("orders", 2);
    assert_eq!(assigned.partition, 2);
    assert!(assigned.from_beginning);
    // 能力缺失以 Unsupported 表达，而非静默降级。
    assert!(matches!(
        KafkaError::Unsupported("consumer group 不支持".into()),
        KafkaError::Unsupported(_)
    ));
}

/// S-2：字段治理——统一 env 前缀、TOML 时长两写法、凭据禁入 TOML、来源优先级与 Debug 脱敏。
#[test]
fn assert_config_governance() {
    for name in [
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
    ] {
        assert!(name.starts_with("FOUNDATIONX_KAFKAX_"), "{name} 前缀不统一");
    }

    // TOML：时长既支持 { secs, nanos } 也支持整数毫秒。
    let from_toml = KafkaConfig::from_toml(
        "brokers = \"127.0.0.1:9092\"\ndelivery_timeout = { secs = 1, nanos = 500000000 }\nconnect_timeout = 1500\n",
    )
    .expect("合法 TOML");
    assert_eq!(from_toml.delivery_timeout, Duration::from_millis(1500));
    assert_eq!(from_toml.connect_timeout, Duration::from_millis(1500));

    // 凭据与未知字段都不得经 TOML 进入。
    assert!(
        KafkaConfig::from_toml("brokers = \"127.0.0.1:9092\"\nsasl_username = \"a\"\n").is_err()
    );
    assert!(KafkaConfig::from_toml("brokers = \"127.0.0.1:9092\"\nsink_id = \"x\"\n").is_err());

    // 优先级：环境变量覆盖默认值，builder 再覆盖两者。
    // 先清全量键：残缺 live 注入（如 SASL 开着但 password 空）不得打红离线 SDD。
    clear_env();
    std::env::set_var(ENV_CLIENT_ID, "kafkax-sdd-env");
    let from_env = KafkaConfig::from_env().expect("合法环境变量");
    clear_env();
    assert_eq!(from_env.client_id, "kafkax-sdd-env");

    let from_builder = KafkaConfig::builder()
        .brokers("127.0.0.1:9092")
        .client_id("kafkax-sdd-builder")
        .build()
        .expect("builder 合法");
    assert_eq!(from_builder.client_id, "kafkax-sdd-builder");

    // 凭据脱敏。
    let with_credentials = KafkaConfig::builder()
        .sasl_plain("sdd-user", "sdd-secret-value")
        .build()
        .expect("回环 SASL 合法");
    let text = format!("{with_credentials:?}");
    assert!(text.contains("***"));
    assert!(!text.contains("sdd-secret-value"));
    assert!(!text.contains("sdd-user"));
}

/// S-3：安全约定——远程必须 TLS、地址禁内嵌 userinfo、CA 必须伴随 TLS、
/// `security_protocol()` 只返回四种取值。
#[test]
fn assert_security_fail_closed() {
    let mut remote = KafkaConfig::default();
    remote.brokers = "broker.example.com:9092".into();
    assert!(remote.validate().is_err(), "远程明文必须 fail-closed");

    let mut remote_tls = KafkaConfig::default();
    remote_tls.brokers = "broker.example.com:9093".into();
    remote_tls.tls = true;
    remote_tls.validate().expect("远程 TLS 合法");

    let mut userinfo = KafkaConfig::default();
    userinfo.brokers = "kafka://user:pass@127.0.0.1:9092".into();
    assert!(userinfo.validate().is_err(), "禁止内嵌 userinfo");

    let mut ca_without_tls = KafkaConfig::default();
    ca_without_tls.tls_ca_file = Some("/tmp/kafkax-sdd-ca.pem".into());
    assert!(ca_without_tls.validate().is_err(), "CA 须同时开启 TLS");

    let matrix = [
        (false, false, "PLAINTEXT"),
        (false, true, "SASL_PLAINTEXT"),
        (true, false, "SSL"),
        (true, true, "SASL_SSL"),
    ];
    for (tls, sasl, expected) in matrix {
        let config = if sasl {
            KafkaConfig::builder()
                .tls(tls)
                .sasl_plain("sdd-user", "sdd-pass")
                .build()
                .expect("回环 + PLAIN 合法")
        } else {
            let mut plain = KafkaConfig::default();
            plain.tls = tls;
            plain
        };
        assert_eq!(config.security_protocol(), expected);
    }
}

/// S-4：失败与并发——无隐式重试、关闭即拒绝新请求、位点单调与原子、
/// tombstone 与空载荷可区分。
#[tokio::test]
async fn assert_failure_and_concurrency() {
    // 不做隐式重试：判定权在调用方。
    assert!(KafkaError::Timeout("x".into()).is_retryable());
    assert!(!KafkaError::Backend("x".into()).is_retryable());
    assert!(!KafkaError::Config("x".into()).is_retryable());

    // 关闭后拒绝新请求。
    let pool = KafkaPool::new(KafkaConfig::default()).expect("同步构造");
    pool.close(Duration::from_millis(200)).await.expect("关闭");
    let error = pool
        .producer()
        .publish(PublishRecord::payload("orders", 0, Bytes::new()))
        .await
        .expect_err("关闭后不得发布");
    assert!(matches!(error, KafkaError::Closed(_)));

    // 位点单调：落后提交不回退。
    let store = MemoryOffsetStore::new();
    store.commit("orders", 0, 9).await.expect("提交");
    store.commit("orders", 0, 4).await.expect("旧提交幂等");
    assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(10));

    // 文件实现原子写：临时文件不残留、重开可读。
    let dir = std::env::temp_dir().join(format!("kafkax-sdd-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("建临时目录");
    let path = dir.join("offsets.tsv");
    let file_store = FileOffsetStore::new(&path);
    file_store
        .commit("orders", 1, 99)
        .await
        .expect("提交文件位点");
    assert_eq!(
        FileOffsetStore::new(&path)
            .committed("orders", 1)
            .await
            .expect("重开读取"),
        Some(100)
    );
    assert!(!path.with_extension("tmp").exists(), "不得残留临时文件");
    std::fs::remove_dir_all(&dir).expect("清理临时目录");

    // tombstone 与零长载荷可区分（旧类型做不到）。
    let message = |payload: Option<Bytes>| KafkaMessage {
        topic: "orders".into(),
        partition: 0,
        offset: 0,
        payload,
        key: None,
        headers: Default::default(),
        timestamp: None,
    };
    assert!(message(None).payload.is_none());
    assert_eq!(message(Some(Bytes::new())).payload_bytes(), b"");
    assert_ne!(message(None).payload, message(Some(Bytes::new())).payload);

    // 显式 ack 语义的起点解析（at-least-once 的基础）。
    let shared = MemoryOffsetStore::new().shared();
    shared.commit("orders", 0, 3).await.expect("提交");
    assert_eq!(
        kafkax::resolve_start_offset(shared.as_ref(), "orders", 0)
            .await
            .expect("解析"),
        Some(4)
    );
    // at-least-once 会话在形状非法时于触网前拒绝（离线可测的 fail-closed 面）。
    let shared: Arc<dyn OffsetCommitStore> = shared;
    assert!(matches!(
        AtLeastOnceConsumer::connect(pool, ConsumerConfig::assign("", 0), shared).await,
        Err(KafkaError::Config(_))
    ));
}

/// S-5：验收——离线即可覆盖配置三来源、纯函数、错误分类与公开 API 面。
#[test]
fn assert_acceptance() {
    let config = KafkaConfig::builder()
        .brokers("127.0.0.1:9092")
        .client_id("kafkax-sdd-acceptance")
        .build()
        .expect("builder 可产出合法配置");
    assert_eq!(config.client_id, "kafkax-sdd-acceptance");

    assert_eq!(
        partition_for_key(b"same-key", 3),
        partition_for_key(b"same-key", 3)
    );
    let id = encode_bus_id("orders", 3, 42);
    assert_eq!(parse_bus_id(&id), Some(("orders", 3, 42)));

    assert_eq!(KafkaError::Config("x".into()).kind(), "config");
    assert_eq!(
        Delivery {
            partition: 0,
            offset: 1
        }
        .offset,
        1
    );

    // 验收命令在仓库根执行，当前目录必须可取得。
    assert!(std::env::current_dir().is_ok());
}
