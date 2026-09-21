//! Kafka 生产者：等待 broker 确认，并受投递超时与关闭信号约束。

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use rskafka::record::Record;
use tokio::sync::watch;

use crate::connection::KafkaPool;
use crate::error::{KafkaError, KafkaResult};
use crate::error_map::map_kafka_error;
use crate::lifecycle::wait_for_shutdown;
use crate::message::{Delivery, PublishRecord};

/// 可克隆的 producer 句柄（共享底层连接池）。
#[derive(Clone, Debug)]
pub struct KafkaProducer {
    pub(crate) pool: KafkaPool,
}

/// produce 在 shutdown / delivery 超时 / 完成 之间的有界等待结果。
#[derive(Debug)]
pub(crate) enum LimitedProduceAwait<T, E> {
    /// 连接池关闭抢先。
    Cancelled,
    /// `delivery_timeout` 到期。
    TimedOut,
    /// produce future 完成。
    Ready(Result<T, E>),
}

/// 在 **shutdown 信号** 与 **投递超时** 之间竞争执行 `produce`。
pub(crate) async fn limited_produce_await<F, Fut, T, E>(
    mut shutdown: watch::Receiver<bool>,
    delivery_timeout: Duration,
    produce: F,
) -> LimitedProduceAwait<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    tokio::select! {
        biased;
        () = wait_for_shutdown(&mut shutdown) => LimitedProduceAwait::Cancelled,
        result = tokio::time::timeout(delivery_timeout, produce()) => match result {
            Ok(inner) => LimitedProduceAwait::Ready(inner),
            Err(_) => LimitedProduceAwait::TimedOut,
        },
    }
}

/// 把 [`limited_produce_await`] 的结果映射为 `Delivery` / 错误，并更新连接池统计。
pub(crate) fn apply_limited_produce_outcome<E>(
    pool: &KafkaPool,
    partition: i32,
    outcome: LimitedProduceAwait<Vec<i64>, E>,
    map_err: impl FnOnce(E) -> KafkaError,
) -> KafkaResult<Delivery> {
    match outcome {
        LimitedProduceAwait::Cancelled => {
            pool.record_publish_cancelled();
            Err(KafkaError::Closed("produce 因连接池关闭而取消".into()))
        }
        LimitedProduceAwait::TimedOut => {
            pool.record_publish_timeout();
            Err(KafkaError::Timeout("produce 等待 broker 确认超时".into()))
        }
        LimitedProduceAwait::Ready(Ok(offsets)) => {
            // `produce` 每次只提交一条记录（见 `KafkaProducer::publish`），因此 broker
            // 必须恰好回一个 offset。**绝不能**在缺失时回退成 `0`：`0` 是合法 offset，
            // 调用方无从分辨，会把「未知位点」当成真实位点写进下游。
            match offsets.as_slice() {
                [offset] => {
                    pool.record_publish_ok();
                    Ok(Delivery {
                        partition,
                        offset: *offset,
                    })
                }
                other => {
                    pool.record_publish_err();
                    Err(KafkaError::Backend(format!(
                        "producer 已收到 broker 确认，但 offset 数量异常（期望 1，实际 {}，分区 {partition}）",
                        other.len()
                    )))
                }
            }
        }
        LimitedProduceAwait::Ready(Err(error)) => {
            pool.record_publish_err();
            Err(map_err(error))
        }
    }
}

impl KafkaProducer {
    /// 发布一条完整记录（topic / 分区 / payload / key / headers）并等待 broker 确认。
    ///
    /// 等待时间受 [`KafkaConfig::delivery_timeout`](crate::KafkaConfig::delivery_timeout)
    /// 约束；连接池关闭会立即取消在途 produce。
    ///
    /// # Errors
    ///
    /// topic 为空、partition 为负、连接池已关闭、投递超时或 broker 返回错误。
    pub async fn publish(&self, record: PublishRecord) -> KafkaResult<Delivery> {
        self.pool
            .ensure_open()
            .map_err(|error| record_if_closed(&self.pool, error))?;
        validate_publish_topic(&record.topic)?;
        if record.partition < 0 {
            return Err(KafkaError::Config("partition 不能为负".into()));
        }
        let client = match self
            .pool
            .partition_client(&record.topic, record.partition)
            .await
        {
            Ok(client) => client,
            Err(error) => return Err(record_if_closed(&self.pool, error)),
        };
        let _operation = self
            .pool
            .start_operation()
            .map_err(|error| record_if_closed(&self.pool, error))?;
        let shutdown = self.pool.shutdown_receiver();
        let wire = Record {
            key: record.key.as_ref().map(|key| key.to_vec()),
            value: Some(record.payload.to_vec()),
            headers: record
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.to_vec()))
                .collect::<BTreeMap<_, _>>(),
            timestamp: Utc::now(),
        };
        let delivery_timeout = self.pool.config().delivery_timeout;
        let outcome = limited_produce_await(shutdown, delivery_timeout, || async {
            client.produce(vec![wire], KafkaPool::compression()).await
        })
        .await;
        let delivery =
            apply_limited_produce_outcome(&self.pool, record.partition, outcome, |error| {
                map_kafka_error("kafkax produce", error)
            })?;
        tracing::debug!(
            topic = %record.topic,
            partition = delivery.partition,
            offset = delivery.offset,
            "kafkax produce 成功"
        );
        Ok(delivery)
    }

    /// 仅 payload 发布到指定分区。
    ///
    /// # Errors
    ///
    /// 与 [`KafkaProducer::publish`] 相同。
    pub async fn publish_to_partition(
        &self,
        topic: &str,
        partition: i32,
        payload: Bytes,
    ) -> KafkaResult<Delivery> {
        self.publish(PublishRecord::payload(topic, partition, payload))
            .await
    }

    /// 带 key 发布到指定分区（可用 [`partition_for_key`](crate::partition_for_key) 选分区）。
    ///
    /// # Errors
    ///
    /// 与 [`KafkaProducer::publish`] 相同。
    pub async fn publish_with_key(
        &self,
        topic: &str,
        partition: i32,
        key: Bytes,
        payload: Bytes,
    ) -> KafkaResult<Delivery> {
        self.publish(PublishRecord::payload(topic, partition, payload).with_key(key))
            .await
    }
}

/// 连接池关闭导致的失败计入 `publish_cancelled`。
fn record_if_closed(pool: &KafkaPool, error: KafkaError) -> KafkaError {
    if matches!(error, KafkaError::Closed(_)) {
        pool.record_publish_cancelled();
    }
    error
}

/// 在 broker I/O 前校验 topic 形状。
fn validate_publish_topic(topic: &str) -> KafkaResult<()> {
    if topic.trim().is_empty() {
        return Err(KafkaError::Config("topic 不能为空".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, KafkaConfigBuilder};

    #[tokio::test]
    async fn publish_after_close_reports_closed_and_counts_cancel() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        pool.close(Duration::from_millis(200)).await.expect("关闭");
        let producer = pool.producer();
        let error = producer
            .publish(PublishRecord::payload("t", 0, Bytes::from_static(b"x")))
            .await
            .expect_err("已关闭");
        assert!(matches!(error, KafkaError::Closed(_)));
        assert!(pool.stats().publish_cancelled >= 1);
    }

    #[tokio::test]
    async fn shape_is_validated_before_broker_io() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        let producer = pool.producer();
        assert!(matches!(
            producer
                .publish(PublishRecord::payload("  ", 0, Bytes::new()))
                .await,
            Err(KafkaError::Config(_))
        ));
        assert!(matches!(
            producer
                .publish(PublishRecord::payload("t", -1, Bytes::new()))
                .await,
            Err(KafkaError::Config(_))
        ));
    }

    /// broker 确认但未回 offset 时**必须报错**，不得伪造 `0`。
    ///
    /// 回归保护：此前实现是 `offsets.first().copied().unwrap_or(0)`，而 `0` 本身
    /// 是合法 offset——调用方无法察觉，会把一个编造的位点当成 broker 分配的真实
    /// 位点。同一 `match` 的其它三个分支（取消 / 超时 / 远端错误）全部返回 `Err`，
    /// 只有这条曾经静默成功。
    #[test]
    fn produce_without_offset_is_an_error_not_a_fabricated_zero() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");

        let empty: LimitedProduceAwait<Vec<i64>, KafkaError> =
            LimitedProduceAwait::Ready(Ok(Vec::new()));
        let error = apply_limited_produce_outcome(&pool, 3, empty, |error| error)
            .expect_err("空 offset 列表必须报错，而不是给出 offset=0");
        assert!(matches!(error, KafkaError::Backend(_)), "{error:?}");
        assert!(
            !error.is_retryable(),
            "协议异常不可重试：重试可能导致重复写入"
        );

        // 数量多于 1 同样异常（每次只提交一条记录）。
        let too_many: LimitedProduceAwait<Vec<i64>, KafkaError> =
            LimitedProduceAwait::Ready(Ok(vec![7, 8]));
        assert!(matches!(
            apply_limited_produce_outcome(&pool, 3, too_many, |error| error),
            Err(KafkaError::Backend(_))
        ));

        // 恰好一个：正常路径，offset 原样透传。
        let one: LimitedProduceAwait<Vec<i64>, KafkaError> =
            LimitedProduceAwait::Ready(Ok(vec![42]));
        let delivery = apply_limited_produce_outcome(&pool, 3, one, |error| error)
            .expect("单条 offset 应成功");
        assert_eq!(delivery.partition, 3);
        assert_eq!(delivery.offset, 42);
    }

    #[test]
    fn empty_topic_rejected() {
        assert!(validate_publish_topic("  ").is_err());
        validate_publish_topic("orders").expect("合法 topic");
    }

    #[tokio::test]
    async fn limited_await_timeout_arm_increments_publish_timeouts() {
        let pool = KafkaPool::new(
            KafkaConfigBuilder::new()
                .delivery_timeout(Duration::from_millis(25))
                .build()
                .expect("配置合法"),
        )
        .expect("配置合法");
        let (_tx, rx) = watch::channel(false);
        let outcome = limited_produce_await(rx, Duration::from_millis(25), || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<Vec<i64>, String>(vec![1])
        })
        .await;
        assert!(matches!(outcome, LimitedProduceAwait::TimedOut));

        let error = apply_limited_produce_outcome(&pool, 0, outcome, KafkaError::Backend)
            .expect_err("超时应失败");
        assert!(matches!(error, KafkaError::Timeout(_)));
        let stats = pool.stats();
        assert_eq!(stats.publish_timeouts, 1);
        assert_eq!(stats.publish_cancelled, 0);
        assert_eq!(stats.publish_failed, 1);
    }

    #[tokio::test]
    async fn limited_await_cancel_arm_increments_publish_cancelled() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            limited_produce_await(rx, Duration::from_secs(60), || async {
                std::future::pending::<Result<Vec<i64>, String>>().await
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(true).expect("广播关闭");

        let outcome = handle.await.expect("等待任务");
        assert!(matches!(outcome, LimitedProduceAwait::Cancelled));
        let error = apply_limited_produce_outcome(&pool, 0, outcome, KafkaError::Backend)
            .expect_err("取消");
        assert!(matches!(error, KafkaError::Closed(_)));
        let stats = pool.stats();
        assert_eq!(stats.publish_cancelled, 1);
        assert_eq!(stats.publish_timeouts, 0);
        assert_eq!(stats.publish_failed, 1);
    }

    #[tokio::test]
    async fn limited_await_ready_arms_update_stats() {
        let pool = KafkaPool::new(KafkaConfig::default()).expect("配置合法");
        let (_tx, rx) = watch::channel(false);
        let outcome = limited_produce_await(rx, Duration::from_secs(1), || async {
            Ok::<Vec<i64>, String>(vec![9])
        })
        .await;
        let delivery =
            apply_limited_produce_outcome(&pool, 2, outcome, KafkaError::Backend).expect("成功");
        assert_eq!(
            delivery,
            Delivery {
                partition: 2,
                offset: 9
            }
        );
        assert_eq!(pool.stats().published, 1);

        let (_tx, rx) = watch::channel(false);
        let outcome = limited_produce_await(rx, Duration::from_secs(1), || async {
            Err::<Vec<i64>, String>("broker boom".into())
        })
        .await;
        assert!(matches!(
            apply_limited_produce_outcome(&pool, 0, outcome, KafkaError::Transient),
            Err(KafkaError::Transient(_))
        ));
        assert_eq!(pool.stats().publish_failed, 1);
    }
}
