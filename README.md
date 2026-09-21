# kafkax

纯 Rust 的 Apache Kafka 适配库，底层使用 [`rskafka`](https://crates.io/crates/rskafka)
（**无 librdkafka / libsasl2 系统依赖**），提供连接池、生产者、显式分区消费者、offset 位点
持久化与健康检查。

- `KafkaPool`：`connect` / `new` / `producer` / `consumer` / `ping` / `health` /
  `health_check` / `stats` / `ensure_topic` / `delete_topic` / `close`
- `KafkaProducer::publish`：等待 broker 确认，受投递超时与关闭信号约束（支持 key / headers）
- `KafkaConsumer`：按**显式分区 + 起始 offset** 流式消费（`ConsumerConfig::assign` +
  `with_start_offset`），不依赖 group coordinator
- `OffsetCommitStore` / `MemoryOffsetStore` / `FileOffsetStore`：应用层位点持久化（文件实现为原子写入）
- `AtLeastOnceConsumer`：显式 `ack` 之后才推进位点，未 ack 即退出会重投
- `KafkaMessage` / `PublishRecord` / `Delivery`、`partition_for_key`、`encode_bus_id` /
  `parse_bus_id`、`map_kafka_error`（可重试判定）、SASL/PLAIN 与 rustls TLS
- `KafkaMessage::payload` 为 `Option<Bytes>`：`None` 表示 **tombstone**（Kafka 的
  null value，compacted topic 的删除标记），与零长载荷**可区分**；只关心字节内容时用
  `payload_bytes()`（tombstone 视作空切片）

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
kafkax = { git = "https://github.com/bytechainx/kafkax" }
```

## 最小可运行示例

```rust,no_run
use bytes::Bytes;
use kafkax::{ConsumerConfig, KafkaConfig, KafkaPool, PublishRecord};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), kafkax::KafkaError> {
    // 1. 建连：默认 127.0.0.1:9092（明文、无凭据），也可用 KafkaConfig::from_toml / builder
    let pool = KafkaPool::connect(KafkaConfig::from_env()?).await?;

    // 2. 生产：等待 broker 确认，返回 (partition, offset)
    let delivery = pool
        .producer()
        .publish(PublishRecord::payload("orders", 0, Bytes::from_static(b"hello")))
        .await?;
    println!("partition={} offset={}", delivery.partition, delivery.offset);

    // 3. 消费：显式分区 + 显式起始 offset（无 consumer group）
    let mut consumer = pool
        .consumer(ConsumerConfig::assign("orders", 0).with_start_offset(0))
        .await?;
    if let Some(message) = consumer.recv_timeout(Duration::from_secs(1)).await? {
        // `payload` 为 `Option<Bytes>`：`None` 表示 tombstone（compacted topic 的删除标记）。
        match &message.payload {
            Some(payload) => println!("bus_id={} payload={payload:?}", message.bus_id()),
            None => println!("bus_id={} tombstone", message.bus_id()),
        }
        // 不需要区分 tombstone 与零长载荷时，用便捷访问器：
        let _bytes = message.payload_bytes();
    }

    // 4. 健康检查 + 优雅关闭（拒绝新请求、取消在途 I/O 并等待在途操作释放）
    let health = pool.health_check().await?;
    println!("ready={} detail={}", health.ready, health.detail);
    pool.close(Duration::from_secs(3)).await?;
    Ok(())
}
```

### at-least-once 用法

```rust,no_run
use std::sync::Arc;

use kafkax::{
    AtLeastOnceConsumer, ConsumerConfig, FileOffsetStore, KafkaConfig, KafkaPool,
    OffsetCommitStore,
};

async fn consume() -> Result<(), kafkax::KafkaError> {
    let pool = KafkaPool::connect(KafkaConfig::from_env()?).await?;
    let store: Arc<dyn OffsetCommitStore> =
        Arc::new(FileOffsetStore::new("/var/lib/kafkax/offsets.tsv"));
    let mut consumer =
        AtLeastOnceConsumer::connect(pool, ConsumerConfig::assign("orders", 0), store).await?;

    while let Some(message) = consumer.recv().await {
        let message = message?;
        // ... 处理 message ...
        consumer.ack().await?; // 只有 ack 成功后位点才前进
    }
    Ok(())
}
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

`KafkaError::is_retryable()` 对连接失败、超时、leader 变更、`UnknownTopicOrPartition`
等返回 `true`；对消息过大、主题非法/不存在、鉴权失败、本地配置与 I/O 错误返回 `false`。

## 配置项

配置来源优先级（低 → 高）：默认值 → TOML（`KafkaConfig::from_toml`）→
环境变量（`KafkaConfig::from_env`）→ `KafkaConfigBuilder`。

| 环境变量（前缀 `FOUNDATIONX_KAFKAX_`） | TOML 字段 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `BROKERS` | `brokers` | `127.0.0.1:9092` | bootstrap，逗号分隔 |
| `CLIENT_ID` | `client_id` | `kafkax` | `client.id` |
| `SASL_MECHANISM` | `sasl_mechanism` | 空（关闭） | 仅支持 `PLAIN`；空串 / `none` 关闭 |
| `SASL_USERNAME` | 不支持（禁止入库） | 无 | SASL 用户名 |
| `SASL_PASSWORD` | 不支持（禁止入库） | 无 | SASL 密码，`Debug` 输出脱敏 |
| `TLS` | `tls` | `false` | `1`/`true`/`yes`/`on` 开启 rustls |
| `TLS_CA_FILE` | `tls_ca_file` | 无（用 `webpki-roots`） | 自定义 PEM CA，须同时开启 TLS |
| `CONNECT_TIMEOUT_MS` | `connect_timeout` | 10s | 建连截止时间 |
| `OPERATION_TIMEOUT_MS` | `operation_timeout` | 10s | 元数据/控制面截止时间 |
| `DELIVERY_TIMEOUT_MS` | `delivery_timeout` | 30s | produce 等待确认截止时间 |

TOML 中时长可写 `{ secs = 30 }` / `{ secs = 1, nanos = 500000000 }`，或直接写整数毫秒
（`connect_timeout = 1500`）。

安全约束（`KafkaConfig::validate`，fail-closed）：

- 远程 broker **必须**启用 TLS（仅回环地址允许明文）
- broker 地址禁止内嵌 userinfo
- 仅接受 SASL/PLAIN，且凭据必须完整；提供了凭据却未启用机制会直接报错
- TOML 文件禁止出现 `sasl_username` / `sasl_password`，凭据只能经环境变量或构建器注入
- `security_protocol()` 返回 `PLAINTEXT` / `SASL_PLAINTEXT` / `SSL` / `SASL_SSL`

TOML 示例：

```toml
brokers = "127.0.0.1:9092"
client_id = "kafkax-demo"
delivery_timeout = { secs = 5 }
operation_timeout = 2500   # 整数毫秒
```

## 测试

```bash
cargo test
```

集成测试全部离线运行：不可达路径统一使用 `127.0.0.1:1`（必然拒绝）与短超时，不依赖真实 broker。

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
