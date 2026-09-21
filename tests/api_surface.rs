#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 公共 API 表面：类型可达性、`Send`/`Sync`、trait 对象与基本行为路径。

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafkax::{
    encode_bus_id, map_kafka_error, parse_bus_id, partition_for_key, resolve_start_offset,
    AtLeastOnceConsumer, ConsumerConfig, Delivery, FileOffsetStore, KafkaConfig, KafkaError,
    KafkaHealth, KafkaMessage, KafkaPool, KafkaPoolStats, KafkaProducer, KafkaResult,
    MemoryOffsetStore, OffsetCommitStore, PublishRecord,
};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}
fn assert_error<E: std::error::Error + Send + Sync + 'static>() {}

#[test]
fn public_types_are_send_and_sync() {
    assert_send::<KafkaPool>();
    assert_sync::<KafkaPool>();
    assert_send::<KafkaProducer>();
    assert_sync::<KafkaProducer>();
    assert_send::<ConsumerConfig>();
    assert_sync::<ConsumerConfig>();
    assert_send::<Delivery>();
    assert_sync::<Delivery>();
    assert_send::<PublishRecord>();
    assert_sync::<PublishRecord>();
    assert_send::<KafkaMessage>();
    assert_sync::<KafkaMessage>();
    assert_send::<KafkaPoolStats>();
    assert_sync::<KafkaPoolStats>();
    assert_send::<KafkaHealth>();
    assert_sync::<KafkaHealth>();
    assert_send::<KafkaConfig>();
    assert_sync::<KafkaConfig>();
    assert_send::<MemoryOffsetStore>();
    assert_sync::<MemoryOffsetStore>();
    assert_send::<FileOffsetStore>();
    assert_sync::<FileOffsetStore>();
    assert_error::<KafkaError>();
}

#[test]
fn consumer_session_types_are_send_and_sync() {
    assert_send::<kafkax::KafkaConsumer>();
    assert_sync::<kafkax::KafkaConsumer>();
    assert_send::<AtLeastOnceConsumer>();
    assert_sync::<AtLeastOnceConsumer>();
}

#[test]
fn offset_store_is_object_safe() {
    let store: Arc<dyn OffsetCommitStore> = Arc::new(MemoryOffsetStore::new());
    assert_eq!(Arc::strong_count(&store), 1);
}

#[tokio::test]
async fn default_exports_have_behavior_paths() {
    // 配置与安全协议
    let config = KafkaConfig::builder()
        .brokers("127.0.0.1:9092")
        .client_id("kafkax-surface")
        .connect_timeout(Duration::from_millis(50))
        .build()
        .expect("loopback 配置合法");
    assert_eq!(config.security_protocol(), "PLAINTEXT");
    assert_eq!(config.client_id, "kafkax-surface");

    // 消费配置
    let consumer_config = ConsumerConfig::assign("surface-topic", 0).with_start_offset(7);
    assert_eq!(consumer_config.partition, 0);
    assert_eq!(consumer_config.start_offset, Some(7));

    // 消息与记录
    let message = KafkaMessage {
        topic: "t".into(),
        partition: 0,
        offset: 1,
        payload: Some(Bytes::from_static(b"x")),
        key: None,
        headers: Default::default(),
        timestamp: None,
    };
    let record = PublishRecord::payload("t", 0, Bytes::from_static(b"y"))
        .with_key(Bytes::from_static(b"k"))
        .header("h", Bytes::from_static(b"1"));
    assert_eq!(record.key.as_ref().map(|key| key.as_ref()), Some(&b"k"[..]));
    assert_eq!(partition_for_key(b"k", 3), partition_for_key(b"k", 3));
    assert_eq!(message.bus_id(), encode_bus_id("t", 0, 1));
    assert!(parse_bus_id(&message.bus_id()).is_some());

    // 交付回执与统计
    assert_eq!(
        Delivery {
            partition: 0,
            offset: 1
        }
        .offset,
        1
    );
    let stats = KafkaPoolStats::default();
    assert_eq!((stats.published, stats.closed), (0, false));

    // offset 存储与 at-least-once 起点解析
    let store = MemoryOffsetStore::new().shared();
    store.commit("t", 0, 3).await.expect("提交位点");
    assert_eq!(store.committed("t", 0).await.expect("读取位点"), Some(4));
    assert_eq!(
        resolve_start_offset(store.as_ref(), "t", 0)
            .await
            .expect("解析起点"),
        Some(4)
    );

    // 错误映射与可重试判定
    let retryable = map_kafka_error("surface", std::io::Error::other("connection refused"));
    assert!(retryable.is_retryable());
    assert!(!KafkaError::Config("bad".into()).is_retryable());

    // 未连接的池：同步构造只做校验
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    assert!(pool.ping().await.is_err());
    let health = pool.health().await.expect("未连接不是致命错误");
    assert!(!health.ready);
    assert!(!pool.stats().closed);
    assert_eq!(pool.config().security_protocol(), "PLAINTEXT");
    pool.close(Duration::from_millis(200)).await.expect("关闭");
    assert!(pool.stats().closed);
}

#[test]
fn kafka_result_alias_is_usable_in_public_signatures() {
    fn classify(value: KafkaResult<()>) -> &'static str {
        match value {
            Ok(()) => "ok",
            Err(error) => error.kind(),
        }
    }
    assert_eq!(classify(Ok(())), "ok");
    assert_eq!(
        classify(Err(KafkaError::Unsupported("x".into()))),
        "unsupported"
    );
}
