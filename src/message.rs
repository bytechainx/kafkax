//! Kafka 消息、生产记录与稳定 broker location 编码。

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use chrono::{DateTime, Utc};

/// 编码 broker location：`topic/partition/offset`。
#[must_use]
pub fn encode_bus_id(topic: &str, partition: i32, offset: i64) -> String {
    format!("{topic}/{partition}/{offset}")
}

/// 解析 `topic/partition/offset`；格式非法返回 `None`。
///
/// topic 中允许出现 `/`（从右向左解析）。
#[must_use]
pub fn parse_bus_id(id: &str) -> Option<(&str, i32, i64)> {
    let mut parts = id.rsplitn(3, '/');
    let offset = parts.next()?.parse().ok()?;
    let partition = parts.next()?.parse().ok()?;
    let topic = parts.next()?;
    if topic.is_empty() {
        return None;
    }
    Some((topic, partition, offset))
}

/// 应用层稳定分区路由：相同 key 在固定 `partitions` 下映射到同一分区。
///
/// 使用 `std::collections::hash_map::DefaultHasher`（固定键 SipHash，**非**随机化），
/// 因此同一进程与同一构建之间结果稳定；这是本库的显式路由辅助，不是 broker 侧的
/// sticky partitioner / murmur2 协议保证。`partitions <= 1` 时返回 `0`。
#[must_use]
pub fn partition_for_key(key: &[u8], partitions: i32) -> i32 {
    if partitions <= 1 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % partitions as u64) as i32
}

/// 生产侧交付回执。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// 目标分区。
    pub partition: i32,
    /// broker 分配的 offset。
    pub offset: i64,
}

/// 生产侧记录（topic / 分区 / 载荷 / key / headers）。
///
/// 经 [`KafkaProducer::publish`](crate::KafkaProducer::publish) 发往 broker。
#[derive(Debug, Clone)]
pub struct PublishRecord {
    /// topic。
    pub topic: String,
    /// 目标分区（由调用方决定；可用 [`partition_for_key`]）。
    pub partition: i32,
    /// 载荷。
    pub payload: Bytes,
    /// 可选 key。
    pub key: Option<Bytes>,
    /// 可选 headers（顺序无关；wire 上为 map）。
    pub headers: BTreeMap<String, Bytes>,
}

impl PublishRecord {
    /// 构造仅含 payload 的记录。
    #[must_use]
    pub fn payload(topic: impl Into<String>, partition: i32, payload: Bytes) -> Self {
        Self {
            topic: topic.into(),
            partition,
            payload,
            key: None,
            headers: BTreeMap::new(),
        }
    }

    /// 设置 key。
    #[must_use]
    pub fn with_key(mut self, key: Bytes) -> Self {
        self.key = Some(key);
        self
    }

    /// 插入单个 header（同名覆盖）。
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: Bytes) -> Self {
        self.headers.insert(name.into(), value);
        self
    }
}

/// 消费到的消息（带 partition/offset/key/headers/timestamp）。
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    /// topic。
    pub topic: String,
    /// 分区。
    pub partition: i32,
    /// offset。
    pub offset: i64,
    /// 载荷；`None` 表示 **tombstone**（Kafka 的 null value，compacted topic 的删除标记）。
    ///
    /// 注意与「空载荷」区分：后者是 [`Some`] 包裹的零长 [`Bytes`]，两者语义不同。
    /// 只关心字节内容、不需要区分二者时用 [`KafkaMessage::payload_bytes`]。
    pub payload: Option<Bytes>,
    /// 可选 key（Kafka 的 key 与 value 同为零可空字段，故二者都是 `Option`）。
    pub key: Option<Bytes>,
    /// 记录 headers。
    pub headers: BTreeMap<String, Bytes>,
    /// 记录时间戳（broker/record 侧）。
    pub timestamp: Option<DateTime<Utc>>,
}

impl KafkaMessage {
    /// 编码为稳定 broker location 字符串。
    #[must_use]
    pub fn bus_id(&self) -> String {
        encode_bus_id(&self.topic, self.partition, self.offset)
    }

    /// 读取 header；不存在返回 `None`。
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&Bytes> {
        self.headers.get(name)
    }

    /// 载荷字节；tombstone（[`payload`](Self::payload) 为 `None`）时返回**空切片**。
    ///
    /// 这是「不区分 tombstone 与空载荷」的便捷路径。需要区分二者的调用方请直接匹配
    /// [`payload`](Self::payload)——只靠 `is_empty()` 无法分辨，因为零长载荷本身
    /// 就是完全合法的 value。
    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        match &self.payload {
            Some(payload) => payload.as_ref(),
            None => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_bus_id() {
        let id = encode_bus_id("orders", 3, 42);
        assert_eq!(id, "orders/3/42");
        let (topic, partition, offset) = parse_bus_id(&id).expect("解析成功");
        assert_eq!((topic, partition, offset), ("orders", 3, 42));
    }

    #[test]
    fn topic_with_slash_roundtrips() {
        let id = encode_bus_id("a/b", 0, 1);
        let (topic, partition, offset) = parse_bus_id(&id).expect("解析成功");
        assert_eq!((topic, partition, offset), ("a/b", 0, 1));
    }

    #[test]
    fn parse_bus_id_rejects_malformed() {
        for id in ["", "onlytopic", "/0/1", "t/x/1", "t/0/x"] {
            assert!(parse_bus_id(id).is_none(), "应拒绝: {id}");
        }
    }

    #[test]
    fn partition_for_key_is_deterministic_and_bounded() {
        assert_eq!(partition_for_key(b"same", 3), partition_for_key(b"same", 3));
        for key in [b"a".as_slice(), b"orders-1", b"long-key-value"] {
            let partition = partition_for_key(key, 5);
            assert!((0..5).contains(&partition), "越界: {partition}");
        }
    }

    #[test]
    fn publish_record_builder_sets_key_and_header() {
        let record = PublishRecord::payload("t", 1, Bytes::from_static(b"p"))
            .with_key(Bytes::from_static(b"k"))
            .header("trace-id", Bytes::from_static(b"1"));
        assert_eq!(record.partition, 1);
        assert_eq!(record.key.as_ref().map(|key| key.as_ref()), Some(&b"k"[..]));
        assert_eq!(
            record.headers.get("trace-id").map(|value| value.as_ref()),
            Some(&b"1"[..])
        );
    }

    /// tombstone 必须能与「空载荷」区分——这正是把 `payload` 改为 `Option<Bytes>` 的原因。
    ///
    /// 回归保护：此前 `payload: Bytes` 配合消费侧的 `record.value.unwrap_or_default()`，
    /// Kafka 的 null value（tombstone）会被读成空串；而零长 value 同样是空串，调用方
    /// 只能靠 `is_empty()` 猜，**猜不出来**。
    #[test]
    fn payload_distinguishes_tombstone_from_empty_value() {
        let message = |payload: Option<Bytes>| KafkaMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            payload,
            key: None,
            headers: BTreeMap::new(),
            timestamp: None,
        };

        let tombstone = message(None);
        let empty_value = message(Some(Bytes::new()));

        // 便捷访问器对二者都给出空切片……
        assert_eq!(tombstone.payload_bytes(), b"");
        assert_eq!(empty_value.payload_bytes(), b"");
        // ……但 `payload` 本身可被区分（旧类型做不到这一点）。
        assert!(tombstone.payload.is_none());
        assert!(empty_value.payload.is_some());
        assert_ne!(tombstone.payload, empty_value.payload);

        assert_eq!(
            message(Some(Bytes::from_static(b"x"))).payload_bytes(),
            b"x"
        );
    }

    #[test]
    fn message_bus_id_and_header_lookup() {
        let message = KafkaMessage {
            topic: "t".into(),
            partition: 2,
            offset: 9,
            payload: Some(Bytes::from_static(b"p")),
            key: Some(Bytes::from_static(b"k")),
            headers: BTreeMap::from([("h".into(), Bytes::from_static(b"v"))]),
            timestamp: None,
        };
        assert_eq!(message.bus_id(), encode_bus_id("t", 2, 9));
        assert_eq!(
            message.header("h").map(|value| value.as_ref()),
            Some(&b"v"[..])
        );
        assert!(message.header("missing").is_none());
    }
}
