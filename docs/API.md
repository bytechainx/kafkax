# kafkax 公开 API

**版本 / 角色**：`kafkax 0.1.0` · 纯 Rust 的 Apache Kafka 适配库（底层 `rskafka`，无 librdkafka / libsasl2 系统依赖）

## 公开消费面

- 配置：`KafkaConfig` / `KafkaConfigBuilder` / `KafkaConfig::from_env` / `KafkaConfig::from_toml` / `KafkaConfig::security_protocol`
- 连接池：`KafkaPool`（`connect` / `new` / `producer` / `consumer` / `ping` / `health` / `health_check` / `stats` / `ensure_topic` / `delete_topic` / `close`），附 `KafkaHealth` / `KafkaPoolStats`
- 生产：`KafkaProducer::publish`（等待 broker 确认，受投递超时与关闭信号约束，支持 key / headers）；`PublishRecord` / `Delivery`
- 消费：`KafkaConsumer` / `ConsumerConfig`（`assign` + `with_start_offset`，显式分区 + 起始 offset，不依赖 group coordinator）；`KafkaMessage`
- 可靠语义：`OffsetCommitStore` / `MemoryOffsetStore` / `FileOffsetStore`（文件实现为原子写入）/ `AtLeastOnceConsumer`（显式 `ack` 后才推进位点）/ `resolve_start_offset`
- 工具：`partition_for_key` / `encode_bus_id` / `parse_bus_id` / `map_kafka_error`；错误类型 `KafkaError` / `KafkaResult`
- `ENV_*` / `DEFAULT_*` 常量：环境变量名与默认值

## 最小用法

```rust,no_run
use bytes::Bytes;
use kafkax::{ConsumerConfig, KafkaConfig, KafkaPool, PublishRecord};
use std::time::Duration;

# async fn demo() -> Result<(), kafkax::KafkaError> {
let pool = KafkaPool::connect(KafkaConfig::from_env()?).await?;

// 生产：等待 broker 确认，返回 (partition, offset)
let delivery = pool
    .producer()
    .publish(PublishRecord::payload("orders", 0, Bytes::from_static(b"hello")))
    .await?;
println!("partition={} offset={}", delivery.partition, delivery.offset);

// 消费：显式分区 + 显式起始 offset（无 consumer group）
let mut consumer = pool
    .consumer(ConsumerConfig::assign("orders", 0).with_start_offset(0))
    .await?;
if let Some(message) = consumer.recv_timeout(Duration::from_secs(1)).await? {
    // `payload` 为 `Option<Bytes>`：`None` 表示 tombstone（compacted topic 的删除标记）
    let _bytes = message.payload_bytes(); // 不区分 tombstone 时的便捷访问器
}

let health = pool.health_check().await?;
println!("ready={} detail={}", health.ready, health.detail);
pool.close(Duration::from_secs(3)).await?;
# Ok(())
# }
```

## 能力边界（NO-GO）

这些能力**刻意不提供**，请勿依赖：

| 能力 | 说明 |
| --- | --- |
| consumer group / rebalance | `rskafka` 无 group coordinator；分区必须由调用方显式指定，位点由应用自管 |
| 事务 / exactly-once | 无 transactional producer，不能跨分区原子写 |
| schema registry | 只处理原始 `bytes`，不做序列化与兼容性校验 |
| 自动重连 / 隐式重试 | 连接失败即返回错误；是否重试由调用方按 `KafkaError::is_retryable()` 决定 |
| 高级 SASL / mTLS | 仅 SASL/PLAIN；TLS 不做客户端证书认证 |

其他约定：

- `KafkaMessage::payload` 为 `Option<Bytes>`：`None` 表示 tombstone，与零长载荷可区分。
- `KafkaError::is_retryable()` 对连接失败、超时、leader 变更、`UnknownTopicOrPartition` 等返回
  `true`；对消息过大、主题非法/不存在、鉴权失败、本地配置与 I/O 错误返回 `false`。
- at-least-once 语义由 `AtLeastOnceConsumer` 提供：未 `ack` 即退出会重投，位点持久化经
  `OffsetCommitStore` 由应用层负责。
