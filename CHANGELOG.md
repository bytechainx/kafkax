# Changelog — kafkax

本文件记录 `kafkax` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/kafka` 抽取而来（抽取时点为 `0.4.5`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 修正

- **TLS crypto provider 吞错**：`build_tls_config()` 此前用 `let _ =` 静默吞掉
  `rustls::crypto::ring::default_provider().install_default()` 的返回值；现改为显式检查
  `CryptoProvider::get_default()` 再安装，安装失败时 fail-fast 返回 `KafkaError::Config`
  而非留到后续 TLS 操作时运行时 panic。已有 provider 时（重复 `connect()` 调用）跳过安装，
  保持幂等。
- **`KafkaError::Io` 细化可重试判定**：`is_retryable()` 此前将 `Io` 一律判为不可重试，
  导致 `FileOffsetStore::commit()` 遭遇瞬态磁盘故障（ENOSPC、EAGAIN、EINTR 等）后
  at-least-once consumer 的 ack 路径卡死。现按 `std::io::ErrorKind` 细化：超时、中断、
  磁盘暂时满、配额暂时超限等瞬态类型判为可重试；文件不存在、权限不足等永久性错误仍不可重试。

## [0.1.2] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，
  把两个门面文件的生产段下沉为子模块。

  **`src/config.rs`（生产段 531 → 206）** —— 新增四个子模块：链式构建器实现
  （`impl KafkaConfigBuilder` 的 11 个方法）→ `src/config/builder.rs`（93 行）；
  环境变量加载层（`from_env`、`apply_env_overlay`、`parse_bool`、`parse_millis`）→
  `src/config/envvars.rs`（96 行）；TOML 解析层（`from_toml`、`toml_error_summary`、
  `reject_secret_keys_in_toml`）→ `src/config/tomlfile.rs`（74 行）；配置校验与 broker host
  解析（`validate`、`broker_host`、`host_is_loopback`）→ `src/config/validate.rs`（100 行）。
  门面保留模块文档、全部 `ENV_*` / `DEFAULT_*` 常量、`KafkaConfig` 与 `KafkaConfigBuilder`
  的**类型定义与字段**、`Default` / `Debug`、`de_duration`、`redact_brokers`、
  `security_protocol` / `builder` / `sasl_credentials` 与**原有内联测试**。
  `de_duration` 刻意留在门面：它是 `#[serde(deserialize_with = …)]` 的目标，该路径按
  **结构体所在模块**解析，随结构体留在一起最稳。本文件**全程无可见性调整** ——
  搬走的项要么原本就是 `pub`，要么只在同一子模块内互调。

  **`src/connection.rs`（生产段 520 → 81）** —— 新增四个子模块：构造面
  （`connect` / `connect_from_env` / `new` / `connect_inner`）与 rustls 配置
  （`build_tls_config`）→ `src/connection/connect.rs`（139 行）；观测面（`config` / `client` /
  `ping` / `health` / `health_check` / `stats`）→ `src/connection/observe.rs`（96 行）；
  运行时面（`close` / `is_closed` 与供 `producer` / `consumer` / `offset` 使用的
  `pub(crate)` 访问器）→ `src/connection/runtime.rs`（112 行）；topic 管理面
  （`ensure_topic` / `delete_topic` 与三个纯函数辅助）→ `src/connection/topic.rs`（128 行）。
  门面保留模块文档、`KafkaPoolStats` / `KafkaHealth` / `KafkaPool` / `PoolInner` 的
  **类型定义与字段**、`impl Debug for PoolInner` 与**原有内联测试**。
  三处可见性放宽（均为 `pub(super)`）：`validate_topic_request` /
  `is_topic_already_exists_error` / `is_topic_missing_error` 被门面内联测试
  （`topic_request_shape_is_validated_before_broker_io`、`topic_error_text_classification`）
  直接驱动，故由测试模块显式导入。其余项要么是 `pub`、要么是 `pub(crate)`、要么只在本模块内
  互调，**无需放宽**。
  子模块名用 `tomlfile` 而非 `toml`（避免 edition 2018 的 uniform path 遮蔽 `toml` 依赖 crate）。

  两处均属**纯搬移**：行多重集比对的「仅旧」**恰为提级的签名**（`config.rs` 为 0 行、`connection.rs`
  为 3 行），**零代码行丢失**；`config.rs` 的内联测试段**逐字节一致**，`connection.rs` 的仅多 6 行
  新增导入。103 项测试与 doctest 结果不变。

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
