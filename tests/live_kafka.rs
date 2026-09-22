#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服（kafkax）：需先 `source /home/workspace/sre/secrets/env/kafkax.env`。
//!
//! 全部用例 `#[ignore]`，默认不跑（CI 行为不变）。显式运行：
//!
//! ```bash
//! set -a; source /home/workspace/sre/secrets/env/kafkax.env; set +a
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_kafka -- --ignored --test-threads=1
//! ```
//!
//! 凭据只从环境变量读取，绝不硬编码；topic 唯一化（pid + 纳秒时间戳）并在收尾删除。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kafkax::{ConsumerConfig, KafkaConfig, KafkaPool, PublishRecord};

/// 进程内唯一的 topic 名：`kafkax_live_<pid>_<纳秒>`。
fn unique_topic() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("kafkax_live_{}_{}", std::process::id(), nanos)
}

/// 建连 → 探活 → 唯一 topic 数据面往返 → 清理 → close。
///
/// kafkax 无 consumer group：分区与起始 offset 均由调用方显式指定，故消费从
/// `delivery.offset` 起读，只取回自己写入的那一条。
#[tokio::test]
#[ignore = "需要真实 Kafka 服务与 FOUNDATIONX_KAFKAX_* 环境变量"]
async fn live_kafka_roundtrip() {
    let config = KafkaConfig::from_env().expect("FOUNDATIONX_KAFKAX_* 必须已注入");
    let pool = KafkaPool::connect(config).await.expect("建连必须成功");
    assert!(!pool.is_closed(), "建连后不应处于关闭态");

    // 探活（结构化）：ready 必须为真，detail 不得为空。
    let health = pool.health_check().await.expect("健康检查必须成功");
    assert!(health.ready, "集群应可达: {}", health.detail);
    assert!(!health.detail.is_empty(), "detail 应给出可读摘要");
    pool.ping().await.expect("ping 必须成功");

    // 唯一 topic，收尾必删。
    let topic = unique_topic();
    pool.ensure_topic(&topic, 1, 1)
        .await
        .expect("幂等建主题必须成功");

    let payload = b"kafkax-live-roundtrip".to_vec();
    let delivery = pool
        .producer()
        .publish(PublishRecord::payload(
            &topic,
            0,
            Bytes::from(payload.clone()),
        ))
        .await
        .expect("发布并等待 broker 确认必须成功");
    assert_eq!(delivery.partition, 0, "发布分区应回显目标分区");
    assert!(delivery.offset >= 0, "broker 必须分配真实 offset");

    let mut consumer = pool
        .consumer(ConsumerConfig::assign(&topic, 0).with_start_offset(delivery.offset))
        .await
        .expect("建立分区消费者必须成功");
    let message = consumer
        .recv_timeout(Duration::from_secs(10))
        .await
        .expect("拉取不得超时")
        .expect("应取回自己写入的那条消息");
    assert_eq!(
        message.payload.as_deref(),
        Some(payload.as_slice()),
        "往返载荷必须逐字节一致"
    );
    assert_eq!(message.offset, delivery.offset, "offset 必须一致");
    assert_eq!(message.bus_id(), format!("{topic}/0/{}", delivery.offset));
    drop(consumer);

    let stats = pool.stats();
    assert!(stats.published >= 1, "成功发布应计入统计");
    assert!(stats.topics_ensured >= 1, "建主题应计入统计");

    // 清理：删主题（不存在视为幂等成功）。
    pool.delete_topic(&topic).await.expect("删除主题必须成功");
    // close() 收尾（E5）：kafkax 的 close 带 deadline 参数，关闭后不得再接受新请求。
    pool.close(Duration::from_secs(5))
        .await
        .expect("关闭必须成功");
    assert!(pool.is_closed(), "close 后必须处于关闭态");
}
