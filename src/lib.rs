//! kafkax — 纯 Rust 的 Apache Kafka 适配库（底层 `rskafka`，无 librdkafka 系统依赖）。
//!
//! 提供连接池、生产者、按显式分区/offset 的消费者、offset 位点持久化与健康检查。
//!
//! # 最小示例
//!
//! ```no_run
//! use bytes::Bytes;
//! use kafkax::{ConsumerConfig, KafkaConfig, KafkaPool, PublishRecord};
//! use std::time::Duration;
//!
//! # async fn demo() -> Result<(), kafkax::KafkaError> {
//! // 1. 按 FOUNDATIONX_KAFKAX_* 环境变量建连（也可用 KafkaConfig::from_toml / builder）
//! let pool = KafkaPool::connect(KafkaConfig::from_env()?).await?;
//!
//! // 2. 发布并等待 broker 确认
//! let delivery = pool
//!     .producer()
//!     .publish(PublishRecord::payload("orders", 0, Bytes::from_static(b"hello")))
//!     .await?;
//! println!("partition={} offset={}", delivery.partition, delivery.offset);
//!
//! // 3. 按显式分区 + 起始 offset 拉取（不依赖 consumer group）
//! let mut consumer = pool.consumer(ConsumerConfig::assign("orders", 0).with_start_offset(0)).await?;
//! if let Some(message) = consumer.recv_timeout(Duration::from_secs(1)).await? {
//!     println!("{}", message.bus_id());
//! }
//!
//! // 4. 健康检查与关闭
//! let health = pool.health_check().await?;
//! println!("ready={} detail={}", health.ready, health.detail);
//! pool.close(Duration::from_secs(3)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 公开 API 一览
//!
//! - 配置：[`KafkaConfig`] / [`KafkaConfigBuilder`] / [`KafkaConfig::from_env`] /
//!   [`KafkaConfig::from_toml`] / [`KafkaConfig::security_protocol`]
//! - 连接池：[`KafkaPool`]（`connect` / `new` / `producer` / `consumer` / `ping` /
//!   `health` / `health_check` / `stats` / `ensure_topic` / `delete_topic` / `close`）
//! - 生产：[`KafkaProducer::publish`] 等待 broker 确认；[`PublishRecord`] / [`Delivery`]
//! - 消费：[`KafkaConsumer`] / [`ConsumerConfig`]；[`KafkaMessage`]
//! - 可靠语义：[`OffsetCommitStore`] / [`MemoryOffsetStore`] / [`FileOffsetStore`] /
//!   [`AtLeastOnceConsumer`] / [`resolve_start_offset`]
//! - 工具：[`partition_for_key`] / [`encode_bus_id`] / [`parse_bus_id`] /
//!   [`map_kafka_error`]；错误类型 [`KafkaError`] / [`KafkaResult`]
//!
//! # 能力边界
//!
//! 本库建立在 `rskafka` 之上，**不提供**以下能力（也不做隐式重试或自动重连）：
//!
//! - **consumer group / rebalance**：没有 group coordinator，位点由应用通过
//!   [`OffsetCommitStore`] 自管；分区由调用方显式指定。
//! - **事务 / EOS**：没有 transactional producer，无法做跨分区原子写。
//! - **schema registry**：只处理原始 `bytes`，不做序列化/兼容性校验。
//! - 高级 SASL 机制（仅 PLAIN）、mTLS 客户端证书。
//!
//! 要获得 at-least-once 语义，请使用 [`AtLeastOnceConsumer`]：显式 `ack` 之后才推进位点。

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

mod at_least_once;
mod config;
mod connection;
mod consumer;
mod error;
mod error_map;
mod lifecycle;
mod message;
mod offset;
mod pool;
mod producer;

pub use at_least_once::{resolve_start_offset, AtLeastOnceConsumer};
pub use config::{
    KafkaConfig, KafkaConfigBuilder, DEFAULT_BROKERS, DEFAULT_SASL_MECHANISM, ENV_BROKERS,
    ENV_CLIENT_ID, ENV_CONNECT_TIMEOUT_MS, ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS,
    ENV_SASL_MECHANISM, ENV_SASL_PASSWORD, ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};
pub use consumer::{ConsumerConfig, KafkaConsumer};
pub use error::{KafkaError, KafkaResult};
pub use error_map::map_kafka_error;
pub use message::{
    encode_bus_id, parse_bus_id, partition_for_key, Delivery, KafkaMessage, PublishRecord,
};
pub use offset::{FileOffsetStore, MemoryOffsetStore, OffsetCommitStore};
pub use pool::{KafkaHealth, KafkaPool, KafkaPoolStats};
pub use producer::KafkaProducer;
