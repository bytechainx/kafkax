# Changelog — kafkax

本文件记录 `kafkax` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/kafka` 抽取而来（抽取时点为 `0.4.5`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.1] - 2026-09-22

### 修正

- 凭据不再经 TOML 错误消息泄漏：`KafkaConfig::from_toml` 此前把 `toml` 的错误原文（含出错行的
  源码片段）插值进 `KafkaError::Config`；当出错行正是承载凭据的那一行（如 `sasl_password = "…`
  引号未闭合，或该行触发未知字段错误）时，凭据片段会被回显进日志与打点。现改为只保留错误摘要、
  行号与字节区间，不回显 TOML 源码。

### 新增

- 特性 002 三类测试面：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部 `TDD-PROBE` 表
  覆盖公开接口契约登记的全部 12 个入口）、`tests/sdd_spec.rs`（`docs/标准.md` 五章 1:1 的
  `SPEC-MAP` 断言）、`tests/aidd_boundary.rs`（9 条对抗/边界用例与 AIDD 复核表）。
- `tests/live_kafka.rs`：真连服往返用例（建连 → 探活 → 唯一 topic 生产/消费 → 删主题 → close），
  恒 `#[ignore]`，凭据只读 `FOUNDATIONX_KAFKAX_*` 环境变量。

## [0.1.0] - 2026-09-21

### 新增

- `KafkaPool`：共享 `rskafka` 客户端句柄的连接池，提供 `connect` / `new` / `producer` /
  `consumer` / `ping` / `health` / `health_check` / `stats` / `ensure_topic` / `delete_topic` /
  `close`，附 `KafkaHealth` / `KafkaPoolStats`。
- `KafkaProducer::publish`：等待 broker 确认的生产路径，受 `delivery_timeout` 与关闭信号约束，
  支持 key 与 headers；返回 `Delivery`（`partition` + `offset`）。
- `KafkaConsumer` / `ConsumerConfig`：按**显式分区 + 起始 offset** 消费（`assign` +
  `with_start_offset`），不依赖 group coordinator。
- 应用层位点持久化：`OffsetCommitStore` trait 与 `MemoryOffsetStore` / `FileOffsetStore`
  （文件实现采用「临时文件 + fsync + rename + 父目录 fsync」的原子写入）。
- `AtLeastOnceConsumer` / `resolve_start_offset`：显式 `ack` 成功后才推进位点，未 ack 即退出
  会重投；支持 `nack_keep_pending` / `drop_pending_unacked`。
- 配置面：`KafkaConfig` / `KafkaConfigBuilder` / `from_env` / `from_toml` /
  `security_protocol` / `validate`（fail-closed），以及 `ENV_*` / `DEFAULT_*` 常量。
- 工具面：`KafkaMessage` / `PublishRecord` / `Delivery` / `partition_for_key` /
  `encode_bus_id` / `parse_bus_id` / `map_kafka_error`，错误类型 `KafkaError` / `KafkaResult`。
- TLS（rustls，默认信任 `webpki-roots`，支持自定义 CA）与 SASL/PLAIN 认证。

### 变更

- 移除对主工程内部 crate（`kernel` / `contracts` 等）的依赖：错误模型从内部错误类型下沉为
  crate 内 `src/error.rs` 的 `KafkaError`（`#[non_exhaustive]` + `is_retryable`），
  生命周期、offset 存储与错误映射全部改为 crate 内自洽实现。
- 凭据注入收敛为环境变量或 builder 两条路径，TOML 明确禁止承载凭据字段。

### 说明

- **不提供** consumer group / rebalance、事务 / exactly-once、schema registry、
  自动重连与隐式重试、高级 SASL（仅 PLAIN）与 mTLS 客户端证书。
- 是否重试一律由调用方按 `KafkaError::is_retryable()` 决定，库本身不做退避或重放。
- `KafkaMessage::payload` 为 `Option<Bytes>`：`None` 表示 tombstone，与零长载荷可区分。
- `connect` 只做校验与构造；可达性由 `ping` / `health_check` 显式探测。
- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
