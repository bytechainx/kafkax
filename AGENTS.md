# kafkax Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

纯 Rust 的 Apache Kafka 适配库，底层使用 `rskafka`（无 librdkafka / libsasl2 系统依赖），提供连接池、生产者、显式分区消费者、offset 位点持久化与健康检查。

## 技术栈

- Rust edition 2021（rust-version 1.85）
- 关键依赖：`rskafka`、`tokio`、`bytes`、`rustls` / `rustls-pemfile` / `webpki-roots`、`serde`、`thiserror`、`toml`、`tracing`、`futures-core` / `futures-util`、`chrono`、`url`
- 不依赖内部框架/私有 crate，零内部耦合，仅使用 crates.io 公开依赖

## 代码结构

```
src/
├── lib.rs            # 入口 + 公共 API re-export（#![deny(missing_docs)] / #![forbid(unsafe_code)]）
├── error.rs          # KafkaError（#[non_exhaustive] + is_retryable）+ KafkaResult 别名
├── error_map.rs      # map_kafka_error：rskafka 错误 → KafkaError 映射
├── config.rs         # KafkaConfig + KafkaConfigBuilder + from_env/from_toml + validate
├── pool.rs           # KafkaPool / KafkaHealth / KafkaPoolStats
├── producer.rs       # KafkaProducer::publish（等待 broker 确认）
├── consumer.rs       # KafkaConsumer + ConsumerConfig（显式分区 + 起始 offset）
├── message.rs        # KafkaMessage / PublishRecord / Delivery + partition_for_key + bus_id 编解码
├── offset.rs         # OffsetCommitStore / MemoryOffsetStore / FileOffsetStore（原子写入）
├── lifecycle.rs      # 建连与关闭生命周期
└── at_least_once.rs  # AtLeastOnceConsumer + resolve_start_offset
tests/
├── api_surface.rs      # 公开 API 面
├── config.rs           # 配置解析与校验
├── offline_failure.rs  # 离线路径失败行为（127.0.0.1:1）
└── pure_functions.rs   # 纯函数工具
benches/
└── hot_path.rs       # 热路径基准（harness = false，支持 --quick）
docs/
├── API.md            # 公开 API 一览与能力边界
└── 标准.md           # 定位、字段治理与验收标准
```

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误类型：thiserror 枚举 + `#[non_exhaustive]` + `pub type KafkaResult<T> = ...`
- 配置：`KafkaConfig` 结构体 + `builder()` + `from_env()` / `from_toml()` + `validate()`（fail-closed）
- SASL 用户名/密码只能经 `FOUNDATIONX_KAFKAX_SASL_USERNAME` / `_SASL_PASSWORD` 环境变量或 builder 注入；TOML 禁止出现凭据字段；`Debug` 输出脱敏
- 远程 broker 必须启用 TLS（仅回环地址允许明文）；仅接受 SASL/PLAIN
- 禁止裸 `unwrap()`（库代码）/ 无注释 `expect()`
- 异步代码使用 tokio，禁止在 async 中做阻塞 I/O
- 不做隐式重试或自动重连；重试判定统一走 `KafkaError::is_retryable()`
- 集成测试全部离线运行：不依赖真实 broker，不可达路径统一用 `127.0.0.1:1` + 短超时

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
