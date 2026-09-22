#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（kafkax）：在**真实** Kafka（SASL_PLAINTEXT 9092）上端到端执行**全部**公开接口。
//!
//! 与 `live_kafka.rs`（单条冒烟往返）不同，本文件的对齐对象是
//! `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! 凭据只从环境变量 `FOUNDATIONX_KAFKAX_*` 读取，不硬编码；topic 唯一化（pid + 纳秒）
//! 并在收尾删除、断言删除生效，绝不触碰既有 topic。
//!
//! ```text
//! set -a; source /home/zone/workspace/sre/secrets/env/kafkax.env; set +a
//! cd /home/workspace/bytechainx/kafkax
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test e2e_kafka -- --ignored --test-threads=1
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kafkax::{
    encode_bus_id, map_kafka_error, parse_bus_id, partition_for_key, resolve_start_offset,
    AtLeastOnceConsumer, ConsumerConfig, Delivery, FileOffsetStore, KafkaConfig,
    KafkaConfigBuilder, KafkaConsumer, KafkaError, KafkaHealth, KafkaMessage, KafkaPool,
    KafkaPoolStats, KafkaProducer, KafkaResult, MemoryOffsetStore, OffsetCommitStore,
    PublishRecord, DEFAULT_BROKERS, DEFAULT_SASL_MECHANISM, ENV_BROKERS, ENV_CLIENT_ID,
    ENV_CONNECT_TIMEOUT_MS, ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS, ENV_SASL_MECHANISM,
    ENV_SASL_PASSWORD, ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};
use rskafka::client::consumer::StartOffset;

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "KafkaError"),
    ("variant", "KafkaError::Backend"),
    ("variant", "KafkaError::Closed"),
    ("variant", "KafkaError::Config"),
    ("variant", "KafkaError::Connection"),
    ("variant", "KafkaError::Io"),
    ("variant", "KafkaError::Serialization"),
    ("variant", "KafkaError::Timeout"),
    ("variant", "KafkaError::Transient"),
    ("variant", "KafkaError::Unsupported"),
    ("fn", "KafkaError::is_retryable"),
    ("fn", "KafkaError::kind"),
    ("type", "AtLeastOnceConsumer"),
    ("fn", "AtLeastOnceConsumer::ack"),
    ("fn", "AtLeastOnceConsumer::committed"),
    ("fn", "AtLeastOnceConsumer::connect"),
    ("fn", "AtLeastOnceConsumer::drop_pending_unacked"),
    ("fn", "AtLeastOnceConsumer::is_terminated"),
    ("fn", "AtLeastOnceConsumer::nack_keep_pending"),
    ("fn", "AtLeastOnceConsumer::partition"),
    ("fn", "AtLeastOnceConsumer::pending"),
    ("fn", "AtLeastOnceConsumer::recv"),
    ("fn", "AtLeastOnceConsumer::recv_timeout"),
    ("fn", "AtLeastOnceConsumer::topic"),
    ("type", "ConsumerConfig"),
    ("field", "ConsumerConfig::from_beginning"),
    ("field", "ConsumerConfig::partition"),
    ("field", "ConsumerConfig::start_offset"),
    ("field", "ConsumerConfig::topic"),
    ("fn", "ConsumerConfig::assign"),
    ("fn", "ConsumerConfig::resolve_start_offset"),
    ("fn", "ConsumerConfig::subscribe"),
    ("fn", "ConsumerConfig::with_start_offset"),
    ("type", "Delivery"),
    ("field", "Delivery::offset"),
    ("field", "Delivery::partition"),
    ("type", "FileOffsetStore"),
    ("fn", "FileOffsetStore::new"),
    ("fn", "FileOffsetStore::path"),
    ("type", "KafkaConfig"),
    ("field", "KafkaConfig::brokers"),
    ("field", "KafkaConfig::client_id"),
    ("field", "KafkaConfig::connect_timeout"),
    ("field", "KafkaConfig::delivery_timeout"),
    ("field", "KafkaConfig::operation_timeout"),
    ("field", "KafkaConfig::sasl_mechanism"),
    ("field", "KafkaConfig::tls"),
    ("field", "KafkaConfig::tls_ca_file"),
    ("fn", "KafkaConfig::builder"),
    ("fn", "KafkaConfig::security_protocol"),
    ("fn", "KafkaConfig::from_env"),
    ("fn", "KafkaConfig::from_toml"),
    ("fn", "KafkaConfig::validate"),
    ("type", "KafkaConfigBuilder"),
    ("fn", "KafkaConfigBuilder::brokers"),
    ("fn", "KafkaConfigBuilder::build"),
    ("fn", "KafkaConfigBuilder::client_id"),
    ("fn", "KafkaConfigBuilder::connect_timeout"),
    ("fn", "KafkaConfigBuilder::delivery_timeout"),
    ("fn", "KafkaConfigBuilder::new"),
    ("fn", "KafkaConfigBuilder::no_sasl"),
    ("fn", "KafkaConfigBuilder::operation_timeout"),
    ("fn", "KafkaConfigBuilder::sasl_plain"),
    ("fn", "KafkaConfigBuilder::tls"),
    ("fn", "KafkaConfigBuilder::tls_ca_file"),
    ("type", "KafkaConsumer"),
    ("fn", "KafkaConsumer::recv"),
    ("fn", "KafkaConsumer::recv_timeout"),
    ("type", "KafkaHealth"),
    ("field", "KafkaHealth::detail"),
    ("field", "KafkaHealth::ready"),
    ("type", "KafkaMessage"),
    ("field", "KafkaMessage::headers"),
    ("field", "KafkaMessage::key"),
    ("field", "KafkaMessage::offset"),
    ("field", "KafkaMessage::partition"),
    ("field", "KafkaMessage::payload"),
    ("field", "KafkaMessage::timestamp"),
    ("field", "KafkaMessage::topic"),
    ("fn", "KafkaMessage::bus_id"),
    ("fn", "KafkaMessage::header"),
    ("fn", "KafkaMessage::payload_bytes"),
    ("type", "KafkaPool"),
    ("fn", "KafkaPool::client"),
    ("fn", "KafkaPool::config"),
    ("fn", "KafkaPool::health"),
    ("fn", "KafkaPool::health_check"),
    ("fn", "KafkaPool::ping"),
    ("fn", "KafkaPool::stats"),
    ("fn", "KafkaPool::close"),
    ("fn", "KafkaPool::is_closed"),
    ("fn", "KafkaPool::connect"),
    ("fn", "KafkaPool::connect_from_env"),
    ("fn", "KafkaPool::new"),
    ("fn", "KafkaPool::consumer"),
    ("fn", "KafkaPool::producer"),
    ("fn", "KafkaPool::delete_topic"),
    ("fn", "KafkaPool::ensure_topic"),
    ("type", "KafkaPoolStats"),
    ("field", "KafkaPoolStats::closed"),
    ("field", "KafkaPoolStats::publish_cancelled"),
    ("field", "KafkaPoolStats::publish_failed"),
    ("field", "KafkaPoolStats::publish_timeouts"),
    ("field", "KafkaPoolStats::published"),
    ("field", "KafkaPoolStats::topics_deleted"),
    ("field", "KafkaPoolStats::topics_ensured"),
    ("type", "KafkaProducer"),
    ("fn", "KafkaProducer::publish"),
    ("fn", "KafkaProducer::publish_to_partition"),
    ("fn", "KafkaProducer::publish_with_key"),
    ("type", "MemoryOffsetStore"),
    ("fn", "MemoryOffsetStore::new"),
    ("fn", "MemoryOffsetStore::put_next"),
    ("fn", "MemoryOffsetStore::shared"),
    ("type", "PublishRecord"),
    ("field", "PublishRecord::headers"),
    ("field", "PublishRecord::key"),
    ("field", "PublishRecord::partition"),
    ("field", "PublishRecord::payload"),
    ("field", "PublishRecord::topic"),
    ("fn", "PublishRecord::header"),
    ("fn", "PublishRecord::payload"),
    ("fn", "PublishRecord::with_key"),
    ("const", "DEFAULT_BROKERS"),
    ("const", "DEFAULT_SASL_MECHANISM"),
    ("const", "ENV_BROKERS"),
    ("const", "ENV_CLIENT_ID"),
    ("const", "ENV_CONNECT_TIMEOUT_MS"),
    ("const", "ENV_DELIVERY_TIMEOUT_MS"),
    ("const", "ENV_OPERATION_TIMEOUT_MS"),
    ("const", "ENV_SASL_MECHANISM"),
    ("const", "ENV_SASL_PASSWORD"),
    ("const", "ENV_SASL_USERNAME"),
    ("const", "ENV_TLS"),
    ("const", "ENV_TLS_CA_FILE"),
    ("type", "OffsetCommitStore"),
    ("fn", "OffsetCommitStore::commit"),
    ("fn", "OffsetCommitStore::committed"),
    ("fn", "encode_bus_id"),
    ("fn", "map_kafka_error"),
    ("fn", "parse_bus_id"),
    ("fn", "partition_for_key"),
    ("fn", "resolve_start_offset"),
    ("type", "KafkaResult"),
];

/// 覆盖登记表：只登记**真实发生**的调用/读取，不登记「计划要调用」。
mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((kind, id)), "清单重复条目：{kind} {id}");
    }
    assert!(!E2E_MANIFEST.is_empty(), "清单不得为空");
}

/// 收尾断言：声明集合与执行集合必须**双向相等**。
fn assert_coverage_complete() {
    let declared: BTreeSet<(&str, &str)> = E2E_MANIFEST.iter().copied().collect();
    let executed = cover::executed();

    let missing: Vec<&(&str, &str)> = declared.difference(&executed).collect();
    let ghost: Vec<&(&str, &str)> = executed.difference(&declared).collect();

    assert!(
        missing.is_empty(),
        "以下 {} 条公开条目被声明却未执行：{missing:?}",
        missing.len()
    );
    assert!(
        ghost.is_empty(),
        "以下 {} 条执行未登记在清单：{ghost:?}",
        ghost.len()
    );
    eprintln!(
        "E2E 覆盖：{}/{} 条公开条目全部执行（kafkax）",
        executed.len(),
        declared.len()
    );
}

/// 进程内唯一的资源名：`<前缀>_<pid>_<纳秒>`。
fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

/// 临时目录（收尾删除并断言删除生效）。
fn unique_temp_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(unique_name(prefix));
    std::fs::create_dir_all(&dir).expect("创建临时目录必须成功");
    dir
}

/// 阶段 1：12 个公开常量逐条取值断言。
///
/// kafkax 没有 `ENV_PREFIX` 常量（只有 10 个具体的 `ENV_*`），故这里把前缀字面量
/// 作为**独立校验基准**写死，再断言每个 `ENV_*` 都以它开头且两两互异。
fn phase_constants() {
    const PREFIX: &str = "FOUNDATIONX_KAFKAX_";
    let env_consts: [(&'static str, &str, &str); 10] = [
        ("ENV_BROKERS", ENV_BROKERS, "FOUNDATIONX_KAFKAX_BROKERS"),
        (
            "ENV_CLIENT_ID",
            ENV_CLIENT_ID,
            "FOUNDATIONX_KAFKAX_CLIENT_ID",
        ),
        (
            "ENV_SASL_MECHANISM",
            ENV_SASL_MECHANISM,
            "FOUNDATIONX_KAFKAX_SASL_MECHANISM",
        ),
        (
            "ENV_SASL_USERNAME",
            ENV_SASL_USERNAME,
            "FOUNDATIONX_KAFKAX_SASL_USERNAME",
        ),
        (
            "ENV_SASL_PASSWORD",
            ENV_SASL_PASSWORD,
            "FOUNDATIONX_KAFKAX_SASL_PASSWORD",
        ),
        ("ENV_TLS", ENV_TLS, "FOUNDATIONX_KAFKAX_TLS"),
        (
            "ENV_TLS_CA_FILE",
            ENV_TLS_CA_FILE,
            "FOUNDATIONX_KAFKAX_TLS_CA_FILE",
        ),
        (
            "ENV_CONNECT_TIMEOUT_MS",
            ENV_CONNECT_TIMEOUT_MS,
            "FOUNDATIONX_KAFKAX_CONNECT_TIMEOUT_MS",
        ),
        (
            "ENV_OPERATION_TIMEOUT_MS",
            ENV_OPERATION_TIMEOUT_MS,
            "FOUNDATIONX_KAFKAX_OPERATION_TIMEOUT_MS",
        ),
        (
            "ENV_DELIVERY_TIMEOUT_MS",
            ENV_DELIVERY_TIMEOUT_MS,
            "FOUNDATIONX_KAFKAX_DELIVERY_TIMEOUT_MS",
        ),
    ];
    let mut seen = BTreeSet::new();
    for (id, value, expected) in env_consts {
        hit("const", id);
        assert!(
            value.starts_with(PREFIX),
            "{id} 必须带前缀 {PREFIX}，实际 {value}"
        );
        assert_eq!(value, expected, "{id} 的取值不得漂移");
        assert!(seen.insert(value), "{id} 与其它常量重复：{value}");
    }
    assert_eq!(seen.len(), 10, "10 个 ENV_* 必须两两互异");

    hit("const", "DEFAULT_BROKERS");
    assert_eq!(DEFAULT_BROKERS, "127.0.0.1:9092");
    hit("const", "DEFAULT_SASL_MECHANISM");
    assert_eq!(DEFAULT_SASL_MECHANISM, "PLAIN");
}

/// 阶段 2：值类型（错误、消息、记录、消费配置、offset 存储、纯函数）。
///
/// `KafkaError` 是 `#[non_exhaustive]`，外部无法穷尽 match；这里改为**逐个显式构造**
/// 全部 9 个变体并断言分类，达到同等覆盖。
async fn phase_value_types() {
    // —— KafkaError：9 个变体逐个构造 + is_retryable / kind ——
    let variants: [(&'static str, KafkaError, bool, &'static str); 9] = [
        (
            "KafkaError::Config",
            KafkaError::Config("e2e".into()),
            false,
            "config",
        ),
        (
            "KafkaError::Connection",
            KafkaError::Connection("e2e".into()),
            true,
            "connection",
        ),
        (
            "KafkaError::Backend",
            KafkaError::Backend("e2e".into()),
            false,
            "backend",
        ),
        (
            "KafkaError::Transient",
            KafkaError::Transient("e2e".into()),
            true,
            "transient",
        ),
        (
            "KafkaError::Serialization",
            KafkaError::Serialization("e2e".into()),
            false,
            "serialization",
        ),
        (
            "KafkaError::Io",
            KafkaError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "e2e-missing",
            )),
            false,
            "io",
        ),
        (
            "KafkaError::Timeout",
            KafkaError::Timeout("e2e".into()),
            true,
            "timeout",
        ),
        (
            "KafkaError::Unsupported",
            KafkaError::Unsupported("e2e".into()),
            false,
            "unsupported",
        ),
        (
            "KafkaError::Closed",
            KafkaError::Closed("e2e".into()),
            false,
            "closed",
        ),
    ];
    // KafkaError 本身是公开类型：上面 9 个变体的真实构造与下面的分类断言即为它的使用。
    hit("type", "KafkaError");
    for (id, error, retryable, kind) in variants {
        hit("variant", id);
        hit("fn", "KafkaError::is_retryable");
        assert_eq!(
            error.is_retryable(),
            retryable,
            "{id} 的可重试分类不符合契约"
        );
        hit("fn", "KafkaError::kind");
        assert_eq!(error.kind(), kind, "{id} 的分类名不符合契约");
        // Display 可用且非空；且不得回显本用例注入的载荷之外的内容。
        assert!(!error.to_string().is_empty());
    }
    // 瞬态 I/O 仍判可重试（ENOSPC/EAGAIN 一类），与上面的 NotFound 形成对照。
    assert!(
        KafkaError::Io(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "e2e-full"
        ))
        .is_retryable(),
        "瞬态磁盘错误必须可重试"
    );

    // —— map_kafka_error：泛型自由函数（`<E>`），旧派生正则曾整条漏掉，现已由核对器
    //    修正并纳入清单（`("fn", "map_kafka_error")`）。用真实驱动错误文本断言映射结果。
    let mapped = map_kafka_error("e2e", std::io::Error::other("request timed out"));
    hit("fn", "map_kafka_error");
    assert!(
        matches!(mapped, KafkaError::Timeout(_)),
        "含 timeout 文本的驱动错误应映射为 Timeout"
    );
    assert!(mapped.is_retryable(), "Timeout 映射结果必须可重试");
    // 对照：不可重试的具体模式（too large）必须映射为 Backend。
    let mapped_permanent = map_kafka_error("e2e", std::io::Error::other("MessageTooLarge"));
    hit("fn", "map_kafka_error");
    assert!(
        matches!(mapped_permanent, KafkaError::Backend(_)),
        "MessageTooLarge 应映射为不可重试的 Backend"
    );
    assert!(!mapped_permanent.is_retryable());
    assert!(
        !mapped_permanent.to_string().contains("MessageTooLarge"),
        "公开错误不得回显驱动原文"
    );

    // —— KafkaResult：成功 + 失败两条路径 ——
    fn as_result(value: u8) -> KafkaResult<u8> {
        Ok(value)
    }
    let ok = as_result(7);
    hit("type", "KafkaResult");
    assert_eq!(ok.expect("Ok 分支"), 7);
    let err: KafkaResult<u8> = Err(KafkaError::Config("e2e".into()));
    assert!(err.is_err());

    // —— ConsumerConfig：4 个构造方法 + 4 个字段 ——
    hit("type", "ConsumerConfig");
    hit("fn", "ConsumerConfig::subscribe");
    let subscribed = ConsumerConfig::subscribe("e2e-topic");
    assert_eq!(subscribed.partition, 0);
    assert!(subscribed.from_beginning);
    assert!(subscribed.start_offset.is_none());
    assert!(matches!(
        subscribed.resolve_start_offset(),
        StartOffset::Earliest
    ));

    hit("fn", "ConsumerConfig::assign");
    let assigned = ConsumerConfig::assign("e2e-topic", 3);
    assert_eq!(assigned.partition, 3);
    assert!(assigned.from_beginning);

    hit("fn", "ConsumerConfig::with_start_offset");
    let with_offset = ConsumerConfig::assign("e2e-topic", 3).with_start_offset(5);
    assert_eq!(with_offset.start_offset, Some(5));
    assert!(
        !with_offset.from_beginning,
        "显式 offset 必须关闭 from_beginning"
    );
    hit("fn", "ConsumerConfig::resolve_start_offset");
    assert!(matches!(
        with_offset.resolve_start_offset(),
        StartOffset::At(5)
    ));

    // 穷尽解构（不写 `..`）：4 个字段全公开，新增字段会在此处编译失败。
    let ConsumerConfig {
        topic,
        partition,
        from_beginning,
        start_offset,
    } = with_offset;
    hit("field", "ConsumerConfig::topic");
    assert_eq!(topic, "e2e-topic");
    hit("field", "ConsumerConfig::partition");
    assert_eq!(partition, 3);
    hit("field", "ConsumerConfig::from_beginning");
    assert!(!from_beginning);
    hit("field", "ConsumerConfig::start_offset");
    assert_eq!(start_offset, Some(5));

    // —— PublishRecord：3 个构建方法 + 5 个字段 ——
    hit("type", "PublishRecord");
    hit("fn", "PublishRecord::payload");
    let record = PublishRecord::payload("e2e-topic", 1, Bytes::from_static(b"payload"));
    assert_eq!(record.partition, 1);
    hit("fn", "PublishRecord::with_key");
    let record = record.with_key(Bytes::from_static(b"key-1"));
    hit("fn", "PublishRecord::header");
    let record = record.header("trace-id", Bytes::from_static(b"t-1"));

    let PublishRecord {
        topic,
        partition,
        payload,
        key,
        headers,
    } = record;
    hit("field", "PublishRecord::topic");
    assert_eq!(topic, "e2e-topic");
    hit("field", "PublishRecord::partition");
    assert_eq!(partition, 1);
    hit("field", "PublishRecord::payload");
    assert_eq!(payload.as_ref(), b"payload");
    hit("field", "PublishRecord::key");
    assert_eq!(key.as_deref(), Some(&b"key-1"[..]));
    hit("field", "PublishRecord::headers");
    assert_eq!(
        headers.get("trace-id").map(Bytes::as_ref),
        Some(&b"t-1"[..])
    );

    // —— Delivery：2 个字段（构造 + 穷尽解构）——
    let delivery = Delivery {
        partition: 2,
        offset: 41,
    };
    hit("type", "Delivery");
    let Delivery { partition, offset } = delivery;
    hit("field", "Delivery::partition");
    assert_eq!(partition, 2);
    hit("field", "Delivery::offset");
    assert_eq!(offset, 41);

    // —— KafkaMessage：3 个方法 + 7 个字段 ——
    let message = KafkaMessage {
        topic: "e2e-topic".into(),
        partition: 0,
        offset: 9,
        payload: Some(Bytes::from_static(b"hello")),
        key: Some(Bytes::from_static(b"k")),
        headers: BTreeMap::from([("h".to_string(), Bytes::from_static(b"v"))]),
        timestamp: None,
    };
    hit("type", "KafkaMessage");
    hit("fn", "KafkaMessage::bus_id");
    assert_eq!(message.bus_id(), "e2e-topic/0/9");
    hit("fn", "KafkaMessage::header");
    assert_eq!(message.header("h").map(Bytes::as_ref), Some(&b"v"[..]));
    assert!(message.header("missing").is_none());
    hit("fn", "KafkaMessage::payload_bytes");
    assert_eq!(message.payload_bytes(), b"hello");
    // tombstone 与零长载荷必须可区分（`payload: Option<Bytes>` 的存在理由）。
    let tombstone = KafkaMessage {
        payload: None,
        ..message.clone()
    };
    assert!(tombstone.payload.is_none());
    assert_eq!(tombstone.payload_bytes(), b"");

    let KafkaMessage {
        topic,
        partition,
        offset,
        payload,
        key,
        headers,
        timestamp,
    } = message;
    hit("field", "KafkaMessage::topic");
    assert_eq!(topic, "e2e-topic");
    hit("field", "KafkaMessage::partition");
    assert_eq!(partition, 0);
    hit("field", "KafkaMessage::offset");
    assert_eq!(offset, 9);
    hit("field", "KafkaMessage::payload");
    assert_eq!(payload.as_deref(), Some(&b"hello"[..]));
    hit("field", "KafkaMessage::key");
    assert_eq!(key.as_deref(), Some(&b"k"[..]));
    hit("field", "KafkaMessage::headers");
    assert_eq!(headers.len(), 1);
    hit("field", "KafkaMessage::timestamp");
    assert!(timestamp.is_none());

    // —— MemoryOffsetStore：3 个方法 + OffsetCommitStore 组合 ——
    hit("type", "MemoryOffsetStore");
    hit("fn", "MemoryOffsetStore::new");
    let memory = MemoryOffsetStore::new();
    hit("fn", "MemoryOffsetStore::put_next");
    memory
        .put_next("e2e-topic", 0, 11)
        .await
        .expect("写入下一位点");
    memory
        .put_next("e2e-topic", 0, 4)
        .await
        .expect("旧位点必须幂等");
    assert!(
        memory.put_next("e2e-topic", 0, -1).await.is_err(),
        "负位点必须被拒绝"
    );
    // 经 trait 读取，验证 put_next 真实落盘（commit/committed 是 trait 方法，不在清单）。
    assert_eq!(
        memory.committed("e2e-topic", 0).await.expect("读取位点"),
        Some(11)
    );
    hit("fn", "MemoryOffsetStore::shared");
    let shared: Arc<MemoryOffsetStore> = memory.shared();

    // —— OffsetCommitStore：以 trait 对象形态被真实使用（trait 声明处的 2 个方法）——
    let store_dyn: Arc<dyn OffsetCommitStore> = shared.clone();
    hit("type", "OffsetCommitStore");
    let via_trait = store_dyn
        .committed("e2e-topic", 0)
        .await
        .expect("trait 读取");
    hit("fn", "OffsetCommitStore::committed");
    assert_eq!(
        via_trait,
        Some(11),
        "trait 读取必须复用 MemoryOffsetStore 的位点"
    );
    store_dyn
        .commit("e2e-topic", 0, 10)
        .await
        .expect("trait 提交");
    hit("fn", "OffsetCommitStore::commit");
    assert_eq!(
        store_dyn
            .committed("e2e-topic", 0)
            .await
            .expect("trait 复核"),
        Some(11),
        "较小的 offset 提交不得回退已提交位点（单调）"
    );
    hit("fn", "OffsetCommitStore::committed");

    // —— resolve_start_offset：自由函数（从 store 解析启动位点）——
    hit("fn", "resolve_start_offset");
    assert_eq!(
        resolve_start_offset(shared.as_ref(), "e2e-topic", 0)
            .await
            .expect("解析启动位点"),
        Some(11)
    );
    assert_eq!(
        resolve_start_offset(shared.as_ref(), "e2e-topic", 7)
            .await
            .expect("无记录分区"),
        None
    );

    // —— FileOffsetStore：new + path + trait 落盘往返 ——
    let dir = unique_temp_dir("kafkax_e2e_offsets");
    let path = dir.join("offsets.tsv");
    hit("type", "FileOffsetStore");
    hit("fn", "FileOffsetStore::new");
    let file_store = FileOffsetStore::new(&path);
    hit("fn", "FileOffsetStore::path");
    assert_eq!(file_store.path(), path.as_path());
    // trait 方法（commit / committed）的文件后端实现路径同样真实执行。
    file_store
        .commit("e2e-topic", 0, 99)
        .await
        .expect("文件位点提交");
    hit("fn", "OffsetCommitStore::commit");
    assert_eq!(
        file_store
            .committed("e2e-topic", 0)
            .await
            .expect("文件位点读取"),
        Some(100)
    );
    hit("fn", "OffsetCommitStore::committed");
    assert!(path.exists(), "提交后位点文件必须存在");
    let reopened = FileOffsetStore::new(&path);
    assert_eq!(
        reopened.committed("e2e-topic", 0).await.expect("重开读取"),
        Some(100)
    );
    std::fs::remove_dir_all(&dir).expect("清理临时目录必须成功");
    assert!(!dir.exists(), "清理后临时目录不得残留");

    // —— 三个自由纯函数 ——
    hit("fn", "encode_bus_id");
    assert_eq!(encode_bus_id("a/b", 3, 42), "a/b/3/42");
    hit("fn", "parse_bus_id");
    assert_eq!(parse_bus_id("a/b/3/42"), Some(("a/b", 3, 42)));
    assert!(parse_bus_id("malformed").is_none());
    hit("fn", "partition_for_key");
    assert_eq!(partition_for_key(b"any", 1), 0, "单分区恒为 0");
    let routed = partition_for_key(b"orders-1", 4);
    assert!((0..4).contains(&routed), "分区必须越界检查：{routed}");
    assert_eq!(
        routed,
        partition_for_key(b"orders-1", 4),
        "同 key 在固定分区数下必须稳定"
    );
}

/// 阶段 3：配置面（`from_env` 读真实注入的 `FOUNDATIONX_KAFKAX_*`）。
///
/// 返回可直连环境的配置；其余构造路径（TOML / 构建器）只做配置层断言，不用于连服务。
fn phase_config_plane() -> KafkaConfig {
    // —— from_env：真实环境变量 ——
    let env_config =
        KafkaConfig::from_env().expect("必须能读取 FOUNDATIONX_KAFKAX_*：请先 source kafkax.env");
    hit("fn", "KafkaConfig::from_env");
    hit("type", "KafkaConfig");
    assert_eq!(
        env_config.brokers, DEFAULT_BROKERS,
        "env 的 bootstrap 地址必须被读到"
    );
    assert!(
        env_config.sasl_mechanism.is_some(),
        "env 必须启用 SASL（SASL_PLAINTEXT）"
    );
    assert!(!env_config.tls, "本环境为 SASL_PLAINTEXT，不得启用 TLS");
    hit("fn", "KafkaConfig::security_protocol");
    assert_eq!(env_config.security_protocol(), "SASL_PLAINTEXT");
    hit("fn", "KafkaConfig::validate");
    env_config.validate().expect("env 配置必须合法");

    // KafkaConfig 含两个**私有**字段（sasl_username / sasl_password），故外部穷尽解构
    // 必须带 `..`；8 个公开字段全部被命名。新增公开字段由核对器的公开面 diff 兜底。
    let KafkaConfig {
        brokers,
        client_id,
        sasl_mechanism,
        tls,
        tls_ca_file,
        delivery_timeout,
        connect_timeout,
        operation_timeout,
        ..
    } = env_config.clone();
    hit("field", "KafkaConfig::brokers");
    assert_eq!(brokers, DEFAULT_BROKERS);
    hit("field", "KafkaConfig::client_id");
    assert!(!client_id.is_empty(), "client_id 不得为空");
    hit("field", "KafkaConfig::sasl_mechanism");
    assert_eq!(sasl_mechanism.as_deref(), Some(DEFAULT_SASL_MECHANISM));
    hit("field", "KafkaConfig::tls");
    assert!(!tls);
    hit("field", "KafkaConfig::tls_ca_file");
    assert!(tls_ca_file.is_none());
    hit("field", "KafkaConfig::delivery_timeout");
    assert!(!delivery_timeout.is_zero());
    hit("field", "KafkaConfig::connect_timeout");
    assert!(!connect_timeout.is_zero());
    hit("field", "KafkaConfig::operation_timeout");
    assert!(!operation_timeout.is_zero());

    // —— from_toml：合法 / 拒绝凭据 / 拒绝未知字段 ——
    let toml_text = r#"
brokers = "127.0.0.1:9092"
client_id = "e2e"
delivery_timeout = { secs = 5 }
connect_timeout = 1500
"#;
    hit("fn", "KafkaConfig::from_toml");
    let from_toml = KafkaConfig::from_toml(toml_text).expect("合法 TOML 必须可解析");
    assert_eq!(from_toml.delivery_timeout, Duration::from_secs(5));
    assert_eq!(from_toml.connect_timeout, Duration::from_millis(1500));
    let secret_error =
        KafkaConfig::from_toml("brokers = \"127.0.0.1:9092\"\nsasl_password = \"hunter2\"\n")
            .expect_err("from_toml 必须拒绝明文凭据字段");
    assert!(
        !secret_error.to_string().contains("hunter2"),
        "错误消息不得回显凭据原文"
    );
    assert!(
        KafkaConfig::from_toml("brokers = \"127.0.0.1:9092\"\nsink_id = \"x\"\n").is_err(),
        "from_toml 必须拒绝未知字段（fail-closed）"
    );

    // —— KafkaConfigBuilder：11 个公开方法 ——
    hit("type", "KafkaConfigBuilder");
    hit("fn", "KafkaConfigBuilder::new");
    let defaults = KafkaConfigBuilder::new().build().expect("默认值必须合法");
    assert_eq!(defaults.brokers, DEFAULT_BROKERS);
    hit("fn", "KafkaConfigBuilder::build");

    let ca_path = std::env::temp_dir().join("kafkax_e2e_ca.pem");
    let built = KafkaConfig::builder()
        .brokers(DEFAULT_BROKERS)
        .client_id("e2e")
        .sasl_plain("e2e-user", "e2e-secret")
        .tls(true)
        .tls_ca_file(ca_path.clone())
        .delivery_timeout(Duration::from_secs(7))
        .connect_timeout(Duration::from_secs(3))
        .operation_timeout(Duration::from_secs(4))
        .build()
        .expect("构建器产出的配置必须合法（CA 路径存在性不参与校验）");
    hit("fn", "KafkaConfig::builder");
    for method in [
        "KafkaConfigBuilder::brokers",
        "KafkaConfigBuilder::client_id",
        "KafkaConfigBuilder::sasl_plain",
        "KafkaConfigBuilder::tls",
        "KafkaConfigBuilder::tls_ca_file",
        "KafkaConfigBuilder::delivery_timeout",
        "KafkaConfigBuilder::connect_timeout",
        "KafkaConfigBuilder::operation_timeout",
        "KafkaConfigBuilder::build",
    ] {
        hit("fn", method);
    }
    assert_eq!(built.client_id, "e2e");
    assert_eq!(built.delivery_timeout, Duration::from_secs(7));
    assert_eq!(built.connect_timeout, Duration::from_secs(3));
    assert_eq!(built.operation_timeout, Duration::from_secs(4));
    assert!(built.tls);
    assert_eq!(built.tls_ca_file.as_deref(), Some(ca_path.as_path()));
    assert_eq!(built.security_protocol(), "SASL_SSL");
    // Debug 必须脱敏凭据。
    let debug = format!("{built:?}");
    assert!(!debug.contains("e2e-secret"), "Debug 不得回显密码");
    assert!(!debug.contains("e2e-user"), "Debug 不得回显用户名");

    // no_sasl：清空机制与凭据。
    hit("fn", "KafkaConfigBuilder::no_sasl");
    let no_sasl = KafkaConfigBuilder::new()
        .sasl_plain("e2e-user", "e2e-secret")
        .no_sasl()
        .build()
        .expect("清除 SASL 后的回环配置合法");
    assert!(no_sasl.sasl_mechanism.is_none());
    assert_eq!(no_sasl.security_protocol(), "PLAINTEXT");

    // —— 构建器 / 校验的 fail-closed 路径 ——
    assert!(
        KafkaConfigBuilder::new().brokers("").build().is_err(),
        "空 brokers 必须被拒绝"
    );
    assert!(
        KafkaConfigBuilder::new().client_id("").build().is_err(),
        "空 client_id 必须被拒绝"
    );
    assert!(
        KafkaConfigBuilder::new()
            .delivery_timeout(Duration::ZERO)
            .build()
            .is_err(),
        "零投递超时必须被拒绝"
    );
    let mut wrong_mechanism = KafkaConfig::builder()
        .sasl_plain("e2e-user", "e2e-secret")
        .build()
        .expect("回环 SASL/PLAIN 合法");
    wrong_mechanism.sasl_mechanism = Some("SCRAM-SHA-256".into());
    assert!(
        wrong_mechanism.validate().is_err(),
        "非 PLAIN 机制必须被拒绝"
    );
    // `KafkaConfig` 有私有字段，无法用「结构体更新语法」在外部构造，故经构建器制造
    // 这两条非法配置（构建器是 crate 提供的唯一外部构造通路）。
    assert!(
        KafkaConfig::builder()
            .brokers("broker.example.com:9092")
            .build()
            .is_err(),
        "远程 broker 未启用 TLS 必须被拒绝"
    );
    assert!(
        KafkaConfig::builder().tls_ca_file(ca_path).build().is_err(),
        "配置 tls_ca_file 但未开启 TLS 必须被拒绝"
    );

    env_config
}

/// 阶段 4：连接池 / 生产者 / 消费者 / at-least-once 在真实服务上的数据面往返。
async fn phase_service_plane(env_config: &KafkaConfig) {
    // —— KafkaPool::connect（含 ping 冒烟）——
    let pool = KafkaPool::connect(env_config.clone())
        .await
        .expect("连接 Kafka 必须成功（检查服务可达性与 FOUNDATIONX_KAFKAX_*）");
    hit("type", "KafkaPool");
    hit("fn", "KafkaPool::connect");
    hit("fn", "KafkaPool::is_closed");
    assert!(!pool.is_closed(), "建连后不应处于关闭态");

    // connect_from_env：独立于显式配置的等价路径。
    let env_pool = KafkaPool::connect_from_env()
        .await
        .expect("connect_from_env 必须成功");
    hit("fn", "KafkaPool::connect_from_env");
    hit("fn", "KafkaPool::ping");
    env_pool.ping().await.expect("env 池 ping 必须成功");
    hit("fn", "KafkaPool::close");
    env_pool
        .close(Duration::from_secs(5))
        .await
        .expect("env 池关闭");
    assert!(env_pool.is_closed());

    // new：只校验、不建连；未连接池的 broker 操作必须报错。
    let unconnected = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    hit("fn", "KafkaPool::new");
    assert!(!unconnected.is_closed());
    hit("fn", "KafkaPool::client");
    assert!(
        unconnected.client().is_err(),
        "未连接池的 client() 必须返回 Connection 错误"
    );
    hit("fn", "KafkaPool::health");
    let unconnected_health = unconnected.health().await.expect("未连接不是致命错误");
    assert!(!unconnected_health.ready, "未连接池不得报 ready");
    hit("fn", "KafkaPool::close");
    unconnected
        .close(Duration::from_millis(200))
        .await
        .expect("关闭未连接池必须成功");
    assert!(unconnected.is_closed());
    assert!(
        unconnected.health().await.is_err(),
        "关闭后 health 必须返回 Closed 错误"
    );

    hit("fn", "KafkaPool::config");
    assert_eq!(pool.config().brokers, env_config.brokers);
    hit("fn", "KafkaPool::client");
    assert!(pool.client().is_ok(), "已连接池的 client() 必须成功");

    hit("fn", "KafkaPool::ping");
    pool.ping().await.expect("ping 必须成功");

    hit("fn", "KafkaPool::health_check");
    let health = pool.health_check().await.expect("健康检查必须成功");
    hit("type", "KafkaHealth");
    let KafkaHealth { ready, detail } = health;
    hit("field", "KafkaHealth::ready");
    assert!(ready, "集群应可达");
    hit("field", "KafkaHealth::detail");
    assert!(
        detail.starts_with("topics="),
        "detail 应为可读摘要：{detail}"
    );

    // —— 唯一 topic：幂等建 → 数据面 → 删 → 断言删除生效 ——
    let topic = unique_name("kafkax_e2e");
    hit("fn", "KafkaPool::ensure_topic");
    pool.ensure_topic(&topic, 1, 1)
        .await
        .expect("幂等建主题必须成功");
    pool.ensure_topic(&topic, 1, 1)
        .await
        .expect("重复建主题必须幂等成功（already-exists 分支）");

    // 生产：3 条记录覆盖 publish / publish_to_partition / publish_with_key。
    hit("type", "KafkaProducer");
    let producer: KafkaProducer = pool.producer();
    hit("fn", "KafkaPool::producer");

    let payload_a = b"kafkax-e2e-alpha".to_vec();
    let key_a = Bytes::from_static(b"key-alpha");
    hit("fn", "KafkaProducer::publish");
    let delivery_a = producer
        .publish(
            PublishRecord::payload(&topic, 0, Bytes::from(payload_a.clone()))
                .with_key(key_a.clone())
                .header("trace-id", Bytes::from_static(b"trace-a")),
        )
        .await
        .expect("publish 并等待 broker 确认必须成功");
    hit("type", "Delivery");
    assert_eq!(delivery_a.partition, 0, "发布分区应回显目标分区");
    assert!(delivery_a.offset >= 0, "broker 必须分配真实 offset");

    let payload_b = b"kafkax-e2e-beta".to_vec();
    hit("fn", "KafkaProducer::publish_to_partition");
    let delivery_b = producer
        .publish_to_partition(&topic, 0, Bytes::from(payload_b.clone()))
        .await
        .expect("publish_to_partition 必须成功");

    let payload_c = b"kafkax-e2e-gamma".to_vec();
    let key_c = Bytes::from_static(b"key-gamma");
    hit("fn", "KafkaProducer::publish_with_key");
    let delivery_c = producer
        .publish_with_key(&topic, 0, key_c.clone(), Bytes::from(payload_c.clone()))
        .await
        .expect("publish_with_key 必须成功");

    // 单分区顺序写：offset 必须逐一递增。
    assert_eq!(delivery_b.offset, delivery_a.offset + 1);
    assert_eq!(delivery_c.offset, delivery_a.offset + 2);

    let Delivery {
        partition: delivery_partition,
        offset: delivery_offset,
    } = delivery_a;
    hit("field", "Delivery::partition");
    assert_eq!(delivery_partition, 0);
    hit("field", "Delivery::offset");
    assert_eq!(delivery_offset, delivery_a.offset);

    // —— KafkaConsumer：显式分区 + 起始 offset ——
    assert!(
        pool.consumer(ConsumerConfig::assign("  ", 0))
            .await
            .is_err(),
        "空 topic 的消费配置必须在 broker I/O 前被拒绝"
    );
    assert!(
        pool.consumer(ConsumerConfig::assign(&topic, -1))
            .await
            .is_err(),
        "负分区必须在 broker I/O 前被拒绝"
    );
    hit("fn", "KafkaPool::consumer");
    let mut consumer: KafkaConsumer = pool
        .consumer(ConsumerConfig::assign(&topic, 0).with_start_offset(delivery_a.offset))
        .await
        .expect("建立分区消费者必须成功");
    hit("type", "KafkaConsumer");

    hit("fn", "KafkaConsumer::recv_timeout");
    let message_a = consumer
        .recv_timeout(Duration::from_secs(10))
        .await
        .expect("拉取不得超时")
        .expect("应取回 alpha 消息");
    assert_eq!(
        message_a.payload.as_deref(),
        Some(payload_a.as_slice()),
        "往返载荷必须逐字节一致"
    );
    assert_eq!(message_a.offset, delivery_a.offset);
    assert_eq!(message_a.key.as_deref(), Some(key_a.as_ref()));
    assert_eq!(
        message_a.header("trace-id").map(Bytes::as_ref),
        Some(&b"trace-a"[..]),
        "headers 必须经 wire 往返"
    );
    assert_eq!(
        message_a.bus_id(),
        format!("{topic}/0/{}", delivery_a.offset)
    );
    assert!(
        message_a.timestamp.is_some(),
        "消费消息必须带 broker 时间戳"
    );
    // 消费侧字段（真实值）逐条读出。
    let KafkaMessage {
        topic: msg_topic,
        partition: msg_partition,
        offset: msg_offset,
        payload: msg_payload,
        key: msg_key,
        headers: msg_headers,
        timestamp: msg_timestamp,
    } = message_a;
    hit("field", "KafkaMessage::topic");
    assert_eq!(msg_topic, topic);
    hit("field", "KafkaMessage::partition");
    assert_eq!(msg_partition, 0);
    hit("field", "KafkaMessage::offset");
    assert_eq!(msg_offset, delivery_a.offset);
    hit("field", "KafkaMessage::payload");
    assert_eq!(msg_payload.as_deref(), Some(payload_a.as_slice()));
    hit("field", "KafkaMessage::key");
    assert_eq!(msg_key.as_deref(), Some(key_a.as_ref()));
    hit("field", "KafkaMessage::headers");
    assert_eq!(msg_headers.len(), 1);
    hit("field", "KafkaMessage::timestamp");
    assert!(msg_timestamp.is_some());

    hit("fn", "KafkaConsumer::recv");
    let message_b = consumer
        .recv()
        .await
        .expect("流未结束")
        .expect("应取回 beta 消息");
    assert_eq!(message_b.offset, delivery_b.offset);
    assert_eq!(message_b.payload.as_deref(), Some(payload_b.as_slice()));

    hit("fn", "KafkaConsumer::recv_timeout");
    let message_c = consumer
        .recv_timeout(Duration::from_secs(10))
        .await
        .expect("拉取不得超时")
        .expect("应取回 gamma 消息");
    assert_eq!(message_c.offset, delivery_c.offset);
    assert_eq!(message_c.payload.as_deref(), Some(payload_c.as_slice()));
    assert_eq!(message_c.key.as_deref(), Some(key_c.as_ref()));
    drop(consumer);

    // —— AtLeastOnceConsumer：真实 at-least-once 语义（显式 ack 才推进位点）——
    let store: Arc<dyn OffsetCommitStore> = MemoryOffsetStore::new().shared();
    hit("type", "AtLeastOnceConsumer");
    hit("fn", "AtLeastOnceConsumer::connect");
    let mut at_least_once = AtLeastOnceConsumer::connect(
        pool.clone(),
        ConsumerConfig::assign(&topic, 0).with_start_offset(delivery_a.offset),
        Arc::clone(&store),
    )
    .await
    .expect("at-least-once 消费者必须建立成功");
    hit("fn", "AtLeastOnceConsumer::topic");
    assert_eq!(at_least_once.topic(), topic);
    hit("fn", "AtLeastOnceConsumer::partition");
    assert_eq!(at_least_once.partition(), 0);
    hit("fn", "AtLeastOnceConsumer::pending");
    assert!(at_least_once.pending().is_none(), "初始无 pending");
    hit("fn", "AtLeastOnceConsumer::is_terminated");
    assert!(!at_least_once.is_terminated());
    hit("fn", "AtLeastOnceConsumer::committed");
    assert_eq!(
        at_least_once.committed().await.expect("读取已提交位点"),
        None,
        "尚未 ack 时 store 中无记录"
    );

    hit("fn", "AtLeastOnceConsumer::recv_timeout");
    let ao_a = at_least_once
        .recv_timeout(Duration::from_secs(10))
        .await
        .expect("拉取不得超时")
        .expect("应取回 alpha 消息");
    assert_eq!(ao_a.offset, delivery_a.offset);
    hit("fn", "AtLeastOnceConsumer::pending");
    assert_eq!(
        at_least_once.pending().map(|message| message.offset),
        Some(delivery_a.offset),
        "交付后必须挂起为 pending"
    );

    hit("fn", "AtLeastOnceConsumer::ack");
    at_least_once.ack().await.expect("ack 必须成功");
    assert!(at_least_once.pending().is_none(), "ack 后 pending 清空");
    hit("fn", "AtLeastOnceConsumer::committed");
    assert_eq!(
        at_least_once.committed().await.expect("读取已提交位点"),
        Some(delivery_a.offset + 1),
        "ack 必须推进为 next-to-read"
    );

    // nack：保留 pending、不提交，下一次 recv 仍返回同一语义位点之后的消息。
    hit("fn", "AtLeastOnceConsumer::nack_keep_pending");
    let ao_b = at_least_once
        .recv()
        .await
        .expect("流未结束")
        .expect("应取回 beta 消息");
    assert_eq!(ao_b.offset, delivery_b.offset);
    at_least_once.nack_keep_pending();
    hit("fn", "AtLeastOnceConsumer::is_terminated");
    assert!(!at_least_once.is_terminated(), "nack 不终止会话");
    hit("fn", "AtLeastOnceConsumer::pending");
    assert_eq!(
        at_least_once.pending().map(|message| message.offset),
        Some(delivery_b.offset),
        "nack 必须保留 pending"
    );
    hit("fn", "AtLeastOnceConsumer::committed");
    assert_eq!(
        at_least_once.committed().await.expect("读取已提交位点"),
        Some(delivery_a.offset + 1),
        "nack 不得推进位点"
    );
    // 未 ack 即重连：新会话从 store 的 next-to-read 重投（at-least-once 的来源）。
    hit("fn", "AtLeastOnceConsumer::connect");
    let mut resumed = AtLeastOnceConsumer::connect(
        pool.clone(),
        ConsumerConfig::assign(&topic, 0),
        Arc::clone(&store),
    )
    .await
    .expect("重连必须成功");
    let replayed = resumed
        .recv_timeout(Duration::from_secs(10))
        .await
        .expect("重连拉取不得超时")
        .expect("重连必须重投未 ack 的 beta 消息");
    assert_eq!(
        replayed.offset, delivery_b.offset,
        "未 ack 的消息必须在重连后重投"
    );
    drop(resumed);

    // drop_pending_unacked：丢弃 pending、不提交并终止会话。
    hit("fn", "AtLeastOnceConsumer::drop_pending_unacked");
    at_least_once.drop_pending_unacked();
    hit("fn", "AtLeastOnceConsumer::is_terminated");
    assert!(at_least_once.is_terminated());
    hit("fn", "AtLeastOnceConsumer::pending");
    assert!(at_least_once.pending().is_none());
    hit("fn", "AtLeastOnceConsumer::committed");
    assert_eq!(
        at_least_once.committed().await.expect("读取已提交位点"),
        Some(delivery_a.offset + 1),
        "drop 不得推进位点"
    );
    hit("fn", "AtLeastOnceConsumer::recv");
    assert!(
        matches!(
            at_least_once.recv().await.expect("有结果"),
            Err(KafkaError::Closed(_))
        ),
        "终止后 recv 必须返回 Closed"
    );
    hit("fn", "AtLeastOnceConsumer::ack");
    assert!(
        matches!(
            at_least_once.ack().await.expect_err("终止后 ack 必须失败"),
            KafkaError::Closed(_)
        ),
        "终止后 ack 必须返回 Closed"
    );
    hit("fn", "AtLeastOnceConsumer::recv_timeout");
    assert!(
        at_least_once
            .recv_timeout(Duration::from_millis(50))
            .await
            .is_err(),
        "终止后 recv_timeout 必须失败"
    );
    drop(at_least_once);

    // 副作用旁证：store 真正落盘（经 trait 读取）。
    assert_eq!(
        store.committed(&topic, 0).await.expect("读取位点"),
        Some(delivery_a.offset + 1)
    );

    // —— 统计快照：7 个字段逐条读出 ——
    hit("type", "KafkaPoolStats");
    hit("fn", "KafkaPool::stats");
    let stats = pool.stats();
    let KafkaPoolStats {
        published,
        publish_failed,
        publish_timeouts,
        publish_cancelled,
        topics_ensured,
        topics_deleted,
        closed,
    } = stats;
    hit("field", "KafkaPoolStats::published");
    assert_eq!(published, 3, "3 次 publish 全部成功");
    hit("field", "KafkaPoolStats::publish_failed");
    assert_eq!(publish_failed, 0, "本阶段无失败发布");
    hit("field", "KafkaPoolStats::publish_timeouts");
    assert_eq!(publish_timeouts, 0);
    hit("field", "KafkaPoolStats::publish_cancelled");
    assert_eq!(publish_cancelled, 0);
    hit("field", "KafkaPoolStats::topics_ensured");
    assert_eq!(topics_ensured, 2, "两次 ensure_topic（含 already-exists）");
    hit("field", "KafkaPoolStats::topics_deleted");
    assert_eq!(topics_deleted, 0, "尚未删除");
    hit("field", "KafkaPoolStats::closed");
    assert!(!closed);
    hit("fn", "KafkaPool::is_closed");
    assert!(!pool.is_closed());

    // —— 清理：删 topic → 断言删除生效（metadata 不再列出）→ 幂等重复删除 ——
    hit("fn", "KafkaPool::delete_topic");
    pool.delete_topic(&topic).await.expect("删除主题必须成功");
    let topics = pool
        .client()
        .expect("已连接池")
        .list_topics()
        .await
        .expect("metadata 拉取必须成功");
    assert!(
        !topics.iter().any(|entry| entry.name == topic),
        "删除后 {topic} 不得再出现在 metadata 中"
    );
    // 幂等：再次删除同一 topic 必须成功（unknown_topic 被视为成功）。
    pool.delete_topic(&topic)
        .await
        .expect("重复删除必须幂等成功");
    let after_delete = pool.stats();
    hit("field", "KafkaPoolStats::topics_deleted");
    assert_eq!(after_delete.topics_deleted, 2, "两次删除均须计入");

    // —— 关停：断言关闭态并拒绝新请求 ——
    hit("fn", "KafkaPool::close");
    pool.close(Duration::from_secs(5))
        .await
        .expect("关闭必须成功");
    hit("fn", "KafkaPool::is_closed");
    assert!(pool.is_closed(), "close 后必须处于关闭态");
    assert!(pool.ping().await.is_err(), "close 后必须拒绝新请求");
    let closed_stats = pool.stats();
    hit("field", "KafkaPoolStats::closed");
    assert!(closed_stats.closed, "关闭后 stats.closed 必须为真");
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
#[tokio::test]
#[ignore = "需要真实 Kafka（SASL_PLAINTEXT 9092）与 FOUNDATIONX_KAFKAX_* 环境变量"]
async fn e2e_kafka_all_public_api() {
    assert_manifest_wellformed();
    phase_constants();
    phase_value_types().await;
    let env_config = phase_config_plane();
    phase_service_plane(&env_config).await;
    assert_coverage_complete();
}
