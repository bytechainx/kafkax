//! 不可达/未连接路径：短超时下 `connect` / `ping` / `consumer` / `ensure_topic` 必须失败。

use std::time::Duration;

use bytes::Bytes;
use kafkax::{ConsumerConfig, KafkaConfig, KafkaError, KafkaPool, PublishRecord};

/// 校验配置，指向必然拒绝连接的地址 `127.0.0.1:1`。
fn refused_config() -> KafkaConfig {
    KafkaConfig::builder()
        .brokers("127.0.0.1:1")
        .client_id("kafkax-offline")
        .connect_timeout(Duration::from_millis(300))
        .operation_timeout(Duration::from_millis(300))
        .delivery_timeout(Duration::from_millis(300))
        .build()
        .expect("loopback 配置合法")
}

#[tokio::test]
async fn connect_to_refused_address_returns_retryable_error() {
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        KafkaPool::connect(refused_config()),
    )
    .await
    .expect("connect 必须受内部截止时间约束");
    let error = result.expect_err("拒绝连接必须失败");
    assert!(error.is_retryable(), "连接失败应可重试: {error}");
    assert!(
        matches!(
            error,
            KafkaError::Connection(_) | KafkaError::Timeout(_) | KafkaError::Transient(_)
        ),
        "分类异常: {error}"
    );
}

#[tokio::test]
async fn connect_invalid_config_fails_before_network() {
    let mut config = KafkaConfig::default();
    config.brokers = "broker.example.com:9092".into();
    let error = KafkaPool::connect(config)
        .await
        .expect_err("远程明文必须 fail-closed");
    assert!(matches!(error, KafkaError::Config(_)));
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn unconnected_pool_rejects_every_io_operation() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");

    assert!(matches!(pool.ping().await, Err(KafkaError::Connection(_))));
    assert!(matches!(
        pool.health_check().await,
        Err(KafkaError::Connection(_))
    ));
    let health = pool.health().await.expect("health 对未连接不报错");
    assert!(!health.ready);
    assert!(
        health.detail.contains("connection"),
        "detail={}",
        health.detail
    );

    assert!(matches!(
        pool.consumer(ConsumerConfig::assign("orders", 0)).await,
        Err(KafkaError::Connection(_))
    ));
    assert!(matches!(
        pool.ensure_topic("orders", 3, 1).await,
        Err(KafkaError::Connection(_))
    ));
    assert!(matches!(
        pool.delete_topic("orders").await,
        Err(KafkaError::Connection(_))
    ));

    let producer = pool.producer();
    assert!(matches!(
        producer
            .publish(PublishRecord::payload(
                "orders",
                0,
                Bytes::from_static(b"x")
            ))
            .await,
        Err(KafkaError::Connection(_))
    ));
    assert!(matches!(
        producer
            .publish_with_key(
                "orders",
                0,
                Bytes::from_static(b"k"),
                Bytes::from_static(b"v")
            )
            .await,
        Err(KafkaError::Connection(_))
    ));

    // 形状非法在接触 broker 之前就失败
    assert!(matches!(
        producer
            .publish_to_partition("orders", -1, Bytes::new())
            .await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        pool.consumer(ConsumerConfig::assign("orders", -1)).await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        pool.ensure_topic("orders", 0, 1).await,
        Err(KafkaError::Config(_))
    ));
    assert!(matches!(
        pool.delete_topic("  ").await,
        Err(KafkaError::Config(_))
    ));
}

#[tokio::test]
async fn closed_pool_rejects_new_operations() {
    let pool = KafkaPool::new(KafkaConfig::default()).expect("默认配置合法");
    pool.close(Duration::from_millis(200))
        .await
        .expect("关闭应成功");
    assert!(pool.is_closed());
    assert!(pool.stats().closed);

    assert!(matches!(pool.ping().await, Err(KafkaError::Closed(_))));
    assert!(matches!(
        pool.health_check().await,
        Err(KafkaError::Closed(_))
    ));
    assert!(matches!(pool.health().await, Err(KafkaError::Closed(_))));
    assert!(matches!(
        pool.consumer(ConsumerConfig::assign("orders", 0)).await,
        Err(KafkaError::Closed(_))
    ));

    let error = pool
        .producer()
        .publish(PublishRecord::payload("orders", 0, Bytes::new()))
        .await
        .expect_err("已关闭");
    assert!(matches!(error, KafkaError::Closed(_)));
    assert!(!error.is_retryable());

    let stats = pool.stats();
    assert!(stats.publish_cancelled >= 1);
    assert!(stats.publish_failed >= 1);

    // 重复关闭是幂等的
    pool.close(Duration::from_millis(200))
        .await
        .expect("重复关闭应成功");
}

#[tokio::test]
async fn tls_config_rejects_missing_ca_file() {
    let config = KafkaConfig::builder()
        .brokers("broker.example.com:9093")
        .tls(true)
        .tls_ca_file("/nonexistent/kafkax-ca.pem")
        .connect_timeout(Duration::from_millis(300))
        .build()
        .expect("配置本身合法");
    let error = KafkaPool::connect(config)
        .await
        .expect_err("CA 文件不存在必须失败");
    assert!(matches!(error, KafkaError::Config(_)), "分类异常: {error}");
}
