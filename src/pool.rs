//! [`KafkaPool`] 的 producer / consumer 工厂门面。
//!
//! 池的类型定义、共享连接状态、生命周期与 topic / 健康操作都在更底层的
//! [`crate::connection`]；本模块只承载需要引用 `consumer` / `producer` 类型的工厂方法，
//! 让依赖方向保持单向：`pool` → `consumer` / `producer` → `connection`，
//! 从而不产生模块环（`MR-DEP-001`）。`crate::pool::KafkaPool` 等路径由下面的
//! 重导出继续提供。

use crate::consumer::{ConsumerConfig, KafkaConsumer};
use crate::error::KafkaResult;
use crate::producer::KafkaProducer;

pub use crate::connection::{KafkaHealth, KafkaPool, KafkaPoolStats};

impl KafkaPool {
    /// 共享 producer 句柄。
    #[must_use]
    pub fn producer(&self) -> KafkaProducer {
        KafkaProducer { pool: self.clone() }
    }

    /// 建立分区消费者。
    ///
    /// # Errors
    ///
    /// 连接池已关闭、消费配置非法或分区客户端建立失败。
    pub async fn consumer(&self, config: ConsumerConfig) -> KafkaResult<KafkaConsumer> {
        self.ensure_open()?;
        KafkaConsumer::connect(self.clone(), config).await
    }
}
