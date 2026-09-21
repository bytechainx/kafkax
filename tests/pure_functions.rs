#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 纯函数与离线行为：稳定分区、bus id 往返、offset 存储、错误可重试判定、消费配置构造。

use bytes::Bytes;
use kafkax::{
    encode_bus_id, map_kafka_error, parse_bus_id, partition_for_key, resolve_start_offset,
    ConsumerConfig, KafkaError, KafkaMessage, MemoryOffsetStore, OffsetCommitStore, PublishRecord,
};

#[test]
fn partition_for_key_is_stable_and_in_range() {
    // 同一 key + 同一分区数 → 同一分区（跨调用稳定，非随机化 hasher）
    for partitions in [2, 7, 16, 64] {
        for key in [b"orders-42".as_slice(), b"user-7", b"", b"a/b"] {
            let first = partition_for_key(key, partitions);
            assert_eq!(first, partition_for_key(key, partitions), "必须稳定");
            assert!(
                (0..partitions).contains(&first),
                "必须在 [0, {partitions}) 内"
            );
        }
    }

    // 退化输入
    assert_eq!(partition_for_key(b"any", 1), 0);
    assert_eq!(partition_for_key(b"any", 0), 0);
    assert_eq!(partition_for_key(b"any", -3), 0);

    // 与源实现一致的锚点值（DefaultHasher = 固定键 SipHash-1-3，非 RandomState）
    assert_eq!(partition_for_key(b"orders-42", 8), 0);
    assert_eq!(partition_for_key(b"user-7", 8), 3);
    assert_eq!(partition_for_key(b"a/b", 3), 2);

    // 不同 key 不应全部塌缩到同一分区（否则等于没有路由）
    let distinct: std::collections::HashSet<i32> = (0..64)
        .map(|index| partition_for_key(index.to_string().as_bytes(), 8))
        .collect();
    assert!(distinct.len() > 1, "不同 key 不应塌缩到同一分区");
}

#[test]
fn bus_id_roundtrip_and_malformed_input() {
    let id = encode_bus_id("orders", 3, 42);
    assert_eq!(id, "orders/3/42");
    assert_eq!(parse_bus_id(&id), Some(("orders", 3, 42)));

    // topic 内含 `/`：从右向左解析
    let nested = encode_bus_id("tenant/orders", 0, 1);
    assert_eq!(parse_bus_id(&nested), Some(("tenant/orders", 0, 1)));

    // 负数 offset / 大 offset
    assert_eq!(
        parse_bus_id(&encode_bus_id("t", 0, i64::MAX)),
        Some(("t", 0, i64::MAX))
    );

    for bad in ["", "onlytopic", "/0/1", "t/x/1", "t/0/x", "t/0"] {
        assert_eq!(parse_bus_id(bad), None, "应拒绝: {bad}");
    }

    let message = KafkaMessage {
        topic: "orders".into(),
        partition: 3,
        offset: 42,
        payload: Some(Bytes::new()),
        key: None,
        headers: Default::default(),
        timestamp: None,
    };
    assert_eq!(message.bus_id(), id);
}

#[tokio::test]
async fn memory_offset_store_commit_semantics() {
    let store = MemoryOffsetStore::new();
    assert_eq!(store.committed("orders", 0).await.expect("读取"), None);

    // next-to-read = delivered + 1
    store.commit("orders", 0, 5).await.expect("提交");
    assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(6));

    // 单调：落后的提交不回退
    store.commit("orders", 0, 2).await.expect("旧提交幂等");
    assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(6));
    store
        .put_next("orders", 0, 3)
        .await
        .expect("旧 put_next 幂等");
    assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(6));

    // 分区隔离
    store.commit("orders", 1, 9).await.expect("提交分区 1");
    assert_eq!(store.committed("orders", 0).await.expect("读取"), Some(6));
    assert_eq!(store.committed("orders", 1).await.expect("读取"), Some(10));

    // 非法位点
    assert!(matches!(
        store.commit("orders", 0, -1).await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        store.commit("orders", 0, i64::MAX).await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        store.put_next("orders", 0, -1).await,
        Err(KafkaError::Config(_))
    ));

    // resolve_start_offset 读取的就是 next-to-read
    let shared = store.shared();
    shared.commit("t", 0, 7).await.expect("提交");
    assert_eq!(
        resolve_start_offset(shared.as_ref(), "t", 0)
            .await
            .expect("解析"),
        Some(8)
    );
    assert_eq!(
        resolve_start_offset(shared.as_ref(), "t", 1)
            .await
            .expect("解析"),
        None
    );
}

#[test]
fn error_retry_classification() {
    // 可重试
    for text in [
        "connection refused",
        "request timed out",
        "Unknown topic or partition",
        "NotLeaderOrFollower",
    ] {
        let error = map_kafka_error("ctx", std::io::Error::other(text));
        assert!(error.is_retryable(), "应可重试: {text} ({error})");
    }
    // 不可重试
    for text in [
        "MessageTooLarge",
        "UNKNOWN_TOPIC_ID",
        "InvalidTopicException",
        "SASL authentication failed",
    ] {
        let error = map_kafka_error("ctx", std::io::Error::other(text));
        assert!(!error.is_retryable(), "不应重试: {text} ({error})");
    }

    assert!(KafkaError::Timeout("x".into()).is_retryable());
    assert!(KafkaError::Transient("x".into()).is_retryable());
    assert!(KafkaError::Connection("x".into()).is_retryable());
    assert!(!KafkaError::Backend("x".into()).is_retryable());
    assert!(!KafkaError::Closed("x".into()).is_retryable());
    assert!(!KafkaError::Unsupported("x".into()).is_retryable());
    assert!(!KafkaError::Serialization("x".into()).is_retryable());
    assert!(!KafkaError::Io(std::io::Error::other("x")).is_retryable());

    // 错误文本不得回显驱动原文
    let error = map_kafka_error(
        "connect",
        std::io::Error::other("host=10.1.2.3 password=hunter2"),
    );
    let text = error.to_string();
    assert!(!text.contains("10.1.2.3"));
    assert!(!text.contains("hunter2"));
}

#[test]
fn consumer_config_construction() {
    let subscribed = ConsumerConfig::subscribe("orders");
    assert_eq!(subscribed.topic, "orders");
    assert_eq!(subscribed.partition, 0);
    assert!(subscribed.from_beginning);
    assert_eq!(subscribed.start_offset, None);
    assert_eq!(
        format!("{:?}", subscribed.resolve_start_offset()),
        "Earliest"
    );

    let assigned = ConsumerConfig::assign("orders", 3);
    assert_eq!(assigned.partition, 3);
    assert_eq!(format!("{:?}", assigned.resolve_start_offset()), "Earliest");

    let explicit = ConsumerConfig::assign("orders", 3).with_start_offset(77);
    assert_eq!(explicit.start_offset, Some(77));
    assert!(
        !explicit.from_beginning,
        "显式 offset 必须覆盖 from_beginning"
    );
    assert_eq!(format!("{:?}", explicit.resolve_start_offset()), "At(77)");

    let latest = ConsumerConfig {
        from_beginning: false,
        ..ConsumerConfig::subscribe("orders")
    };
    assert_eq!(format!("{:?}", latest.resolve_start_offset()), "Latest");
}

#[test]
fn publish_record_builder() {
    let record = PublishRecord::payload("orders", 2, Bytes::from_static(b"payload"))
        .with_key(Bytes::from_static(b"key"))
        .header("trace-id", Bytes::from_static(b"abc"))
        .header("trace-id", Bytes::from_static(b"def"));
    assert_eq!(record.topic, "orders");
    assert_eq!(record.partition, 2);
    assert_eq!(record.payload.as_ref(), b"payload");
    assert_eq!(
        record.key.as_ref().map(|key| key.as_ref()),
        Some(&b"key"[..])
    );
    assert_eq!(record.headers.len(), 1, "同名 header 覆盖");
    assert_eq!(
        record.headers.get("trace-id").map(|value| value.as_ref()),
        Some(&b"def"[..])
    );

    let message = KafkaMessage {
        topic: "orders".into(),
        partition: 0,
        offset: 0,
        payload: Some(Bytes::new()),
        key: None,
        headers: record.headers.clone(),
        timestamp: None,
    };
    assert_eq!(
        message.header("trace-id").map(|value| value.as_ref()),
        Some(&b"def"[..])
    );
    assert!(message.header("missing").is_none());
}

#[tokio::test]
async fn offset_commit_store_is_usable_as_trait_object() {
    let store: Box<dyn OffsetCommitStore> = Box::new(MemoryOffsetStore::new());
    store.commit("t", 0, 1).await.expect("提交");
    assert_eq!(store.committed("t", 0).await.expect("读取"), Some(2));

    let shared: std::sync::Arc<dyn OffsetCommitStore> =
        std::sync::Arc::new(MemoryOffsetStore::new());
    shared.commit("t", 0, 4).await.expect("提交");
    assert_eq!(
        resolve_start_offset(shared.as_ref(), "t", 0)
            .await
            .expect("解析"),
        Some(5)
    );
}
