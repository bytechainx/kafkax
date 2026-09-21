#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: TOML 承载凭据且值含特殊字符 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 凭据禁入 TOML 且错误不得回显 | 结论=保留
//! // AIDD: broker 地址内嵌 userinfo | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 禁止内嵌 userinfo | 结论=保留
//! // AIDD: 时长 nanos 越界（10^9） | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 时长字段表示范围 | 结论=保留
//! // AIDD: 全分隔符 brokers 与 IPv6 回环 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 至少一个有效地址 / 回环允许明文 | 结论=保留
//! // AIDD: 分区路由 partitions<=1 与超长 key、topic 含斜杠的 bus_id | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 应用层稳定路由 | 结论=保留
//! // AIDD: tombstone 与零长载荷不可区分 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 payload 为 Option<Bytes> | 结论=保留
//! // AIDD: 发布前负分区 / 空白 topic | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 形状校验先于 broker I/O | 结论=保留
//! // AIDD: 提供了凭据却未启用 SASL 机制 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 凭据不得被静默忽略 | 结论=保留

use std::time::Duration;

use bytes::Bytes;
use kafkax::{
    encode_bus_id, parse_bus_id, partition_for_key, KafkaConfig, KafkaError, KafkaMessage,
    KafkaPool, PublishRecord,
};

/// 边界：TOML 中的凭据值含引号转义等特殊字符时，既必须拒绝、又不得把值回显进错误。
#[test]
fn toml_secret_error_never_echoes_value() {
    let secret = "p@ss\\\"word-specials";
    let text = format!("brokers = \"127.0.0.1:9092\"\nsasl_password = \"{secret}\"\n");
    let error = KafkaConfig::from_toml(&text).expect_err("凭据字段必须被拒绝");
    assert!(matches!(error, KafkaError::Config(_)), "{error}");
    let message = error.to_string();
    assert!(!message.contains(secret), "错误消息回显了凭据: {message}");
    assert!(
        !message.contains("word-specials"),
        "错误消息含片段: {message}"
    );

    let username = "brokers = \"127.0.0.1:9092\"\nsasl_username = \"admin\"\n";
    let error = KafkaConfig::from_toml(username).expect_err("用户名同样禁入 TOML");
    assert!(!error.to_string().contains("admin"), "{error}");
}

/// 边界：broker 地址内嵌 userinfo 必须被拒绝，且 `Debug` 输出脱敏该片段。
#[test]
fn broker_userinfo_is_rejected_and_redacted() {
    let mut config = KafkaConfig::default();
    config.brokers = "kafka://embedded-user:embedded-secret@localhost:9092".into();
    assert!(config.validate().is_err(), "内嵌 userinfo 必须被拒绝");

    let text = format!("{config:?}");
    assert!(!text.contains("embedded-user"), "Debug 泄露用户名: {text}");
    assert!(!text.contains("embedded-secret"), "Debug 泄露密码: {text}");
    assert!(
        text.contains("<redacted-userinfo>"),
        "应出现脱敏占位: {text}"
    );
}

/// 边界：TOML 时长 nanos 越界（10^9）必须失败，而 999_999_999 合法。
#[test]
fn duration_nanos_out_of_range_is_rejected() {
    let out_of_range =
        "brokers = \"127.0.0.1:9092\"\nconnect_timeout = { secs = 1, nanos = 1000000000 }\n";
    let error = KafkaConfig::from_toml(out_of_range).expect_err("nanos 越界必须失败");
    assert!(matches!(error, KafkaError::Config(_)), "{error}");

    let at_bound =
        "brokers = \"127.0.0.1:9092\"\nconnect_timeout = { secs = 1, nanos = 999999999 }\n";
    let config = KafkaConfig::from_toml(at_bound).expect("上界合法");
    assert_eq!(
        config.connect_timeout,
        Duration::from_secs(1) + Duration::from_nanos(999_999_999)
    );
}

/// 边界：全分隔符 brokers 无有效地址必须失败；IPv6 回环允许明文。
#[test]
fn separator_only_and_ipv6_brokers() {
    let mut separators = KafkaConfig::default();
    separators.brokers = " , , ".into();
    assert!(separators.validate().is_err(), "无有效地址必须被拒绝");

    let mut ipv6 = KafkaConfig::default();
    ipv6.brokers = "[::1]:9092".into();
    ipv6.validate().expect("IPv6 回环允许明文");

    let mut empty = KafkaConfig::default();
    empty.brokers = "   ".into();
    assert!(empty.validate().is_err(), "空白 brokers 必须被拒绝");
}

/// 边界：分区路由在 `partitions <= 1` 时恒为 0、结果有界且确定；bus_id 支持 topic 含斜杠。
#[test]
fn partition_for_key_and_bus_id_boundaries() {
    assert_eq!(partition_for_key(b"anything", 0), 0);
    assert_eq!(partition_for_key(b"anything", 1), 0);
    assert_eq!(partition_for_key(b"anything", -5), 0);

    let long_key = vec![b'k'; 100_000];
    let partition = partition_for_key(&long_key, 7);
    assert!((0..7).contains(&partition), "越界: {partition}");
    assert_eq!(
        partition,
        partition_for_key(&long_key, 7),
        "同一 key 必须稳定"
    );

    let id = encode_bus_id("a/b/c", -1, 0);
    assert_eq!(id, "a/b/c/-1/0");
    assert_eq!(parse_bus_id(&id), Some(("a/b/c", -1, 0)));
    for malformed in ["", "onlytopic", "/0/1", "t/x/1", "t/0/x"] {
        assert!(parse_bus_id(malformed).is_none(), "应拒绝: {malformed}");
    }
}

/// 边界：tombstone（`None`）与零长载荷（`Some(空)`）必须可区分——旧类型做不到这一点。
#[test]
fn tombstone_distinguishable_from_empty_payload() {
    let message = |payload: Option<Bytes>| KafkaMessage {
        topic: "orders".into(),
        partition: 0,
        offset: 0,
        payload,
        key: None,
        headers: Default::default(),
        timestamp: None,
    };

    let tombstone = message(None);
    let empty = message(Some(Bytes::new()));
    assert_eq!(tombstone.payload_bytes(), b"");
    assert_eq!(empty.payload_bytes(), b"");
    assert_ne!(tombstone.payload, empty.payload);
    assert!(tombstone.payload.is_none());
    assert!(empty.payload.is_some());
}

/// 边界：形状非法（负分区 / 空白 topic）在触网前就被拒绝，与池是否连接无关。
#[tokio::test]
async fn publish_shape_rejected_before_broker_io() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("同步构造");
    let producer = pool.producer();

    assert!(matches!(
        producer
            .publish(PublishRecord::payload("orders", -1, Bytes::new()))
            .await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        producer
            .publish(PublishRecord::payload("\t ", 0, Bytes::new()))
            .await,
        Err(KafkaError::Config(_))
    ));
}

/// 边界：提供了凭据却把机制置空，必须 fail-closed，不得静默忽略凭据。
#[test]
fn credentials_without_mechanism_fail_closed() {
    let mut config = KafkaConfig::builder()
        .sasl_plain("aidd-user", "aidd-pass")
        .build()
        .expect("回环 SASL 合法");
    config.sasl_mechanism = None;
    assert!(config.validate().is_err(), "凭据存在但未启用机制必须被拒绝");

    let mut unknown = KafkaConfig::default();
    unknown.sasl_mechanism = Some("SCRAM-SHA-256".into());
    assert!(unknown.validate().is_err(), "仅支持 SASL/PLAIN");
}
