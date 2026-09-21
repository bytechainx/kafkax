//! `rskafka` 错误 → [`KafkaError`] 映射与可重试判定。
//!
//! 分类优先使用结构化信息（Kafka 协议错误码）：`rskafka::client::error::ProtocolError`。
//! 无法结构化识别时回退到错误文本启发式。公开错误只携带分类摘要与上下文，
//! **不回显驱动原文**（驱动原文可能包含主机、账号等信息）。

use std::error::Error;

use rskafka::client::error::{Error as RskafkaError, ProtocolError};

use crate::error::KafkaError;

/// 将任意驱动错误映射为 [`KafkaError`]。
///
/// 若 `err` 是 `rskafka` 错误，按协议错误码精确分类；否则按错误文本启发式分类。
/// 调用方可用 [`KafkaError::is_retryable`] 决定是否重试（本库不做隐式重试）。
#[must_use]
pub fn map_kafka_error<E>(context: &str, err: E) -> KafkaError
where
    E: Error + Send + Sync + 'static,
{
    let error: &dyn Error = &err;
    let classified = error
        .downcast_ref::<RskafkaError>()
        .map_or_else(|| classify_message(&err.to_string()), classify_rskafka);
    match classified {
        Classified::Connection => KafkaError::Connection(format!("{context}: 驱动连接失败")),
        Classified::Backend => KafkaError::Backend(format!("{context}: 驱动拒绝请求")),
        Classified::Transient => KafkaError::Transient(format!("{context}: 驱动报告可重试故障")),
        Classified::Timeout => KafkaError::Timeout(format!("{context}: 驱动请求超时")),
        Classified::Closed => KafkaError::Closed(format!("{context}: 驱动请求已取消")),
    }
}

/// 错误分类（不含上下文）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Classified {
    /// 连接类故障，可重试。
    Connection,
    /// 请求被远端拒绝，不可重试。
    Backend,
    /// 远端瞬时故障，可重试。
    Transient,
    /// 超时，可重试。
    Timeout,
    /// 连接池关闭/请求取消，不可重试。
    Closed,
}

/// 按 `rskafka` 错误结构分类。
fn classify_rskafka(error: &RskafkaError) -> Classified {
    match error {
        RskafkaError::Connection(_) | RskafkaError::Request(_) => Classified::Connection,
        RskafkaError::RetryFailed(_) => Classified::Transient,
        RskafkaError::Timeout => Classified::Timeout,
        RskafkaError::InvalidResponse(_) => Classified::Backend,
        RskafkaError::ServerError { protocol_error, .. } => classify_protocol(*protocol_error),
        _ => Classified::Backend,
    }
}

/// 按 Kafka 协议错误码分类。
///
/// `ProtocolError` 为 `#[non_exhaustive]`：未显式列出的码一律按不可重试处理，
/// 避免对未识别错误盲目重试。
fn classify_protocol(error: ProtocolError) -> Classified {
    match error {
        ProtocolError::UnknownServerError
        | ProtocolError::LeaderNotAvailable
        | ProtocolError::NotLeaderOrFollower
        | ProtocolError::RequestTimedOut
        | ProtocolError::BrokerNotAvailable
        | ProtocolError::ReplicaNotAvailable
        | ProtocolError::NetworkException
        | ProtocolError::KafkaStorageError
        | ProtocolError::NotEnoughReplicas
        | ProtocolError::NotEnoughReplicasAfterAppend
        | ProtocolError::StaleControllerEpoch
        | ProtocolError::NotController
        | ProtocolError::UnknownTopicOrPartition
        | ProtocolError::UnknownLeaderEpoch
        | ProtocolError::FencedLeaderEpoch
        | ProtocolError::StaleBrokerEpoch
        | ProtocolError::PreferredLeaderNotAvailable
        | ProtocolError::EligibleLeadersNotAvailable
        | ProtocolError::UnstableOffsetCommit
        | ProtocolError::ThrottlingQuotaExceeded
        | ProtocolError::ConcurrentTransactions
        | ProtocolError::OutOfOrderSequenceNumber
        | ProtocolError::DuplicateSequenceNumber
        | ProtocolError::ReassignmentInProgress
        | ProtocolError::InvalidProducerEpoch
        | ProtocolError::UnknownProducerId => Classified::Transient,
        // 不可重试：请求本身非法、主题/资源不存在、权限不足、位点需显式重置等。
        ProtocolError::MessageTooLarge
        | ProtocolError::RecordListTooLarge
        | ProtocolError::OffsetMetadataTooLarge
        | ProtocolError::InvalidTopicException
        | ProtocolError::UnknownTopicId
        | ProtocolError::ResourceNotFound
        | ProtocolError::TopicAlreadyExists
        | ProtocolError::InvalidPartitions
        | ProtocolError::InvalidReplicationFactor
        | ProtocolError::InvalidReplicaAssignment
        | ProtocolError::InvalidConfig
        | ProtocolError::InvalidRequest
        | ProtocolError::InvalidRecord
        | ProtocolError::CorruptMessage
        | ProtocolError::InvalidFetchSize
        | ProtocolError::InvalidTimestamp
        | ProtocolError::UnsupportedForMessageFormat
        | ProtocolError::UnsupportedVersion
        | ProtocolError::UnsupportedCompressionType
        | ProtocolError::PolicyViolation
        | ProtocolError::TopicAuthorizationFailed
        | ProtocolError::GroupAuthorizationFailed
        | ProtocolError::ClusterAuthorizationFailed
        | ProtocolError::TransactionalIdAuthorizationFailed
        | ProtocolError::SaslAuthenticationFailed
        | ProtocolError::UnsupportedSaslMechanism
        | ProtocolError::IllegalSaslState
        | ProtocolError::UnacceptableCredential
        | ProtocolError::SecurityDisabled
        | ProtocolError::TopicDeletionDisabled
        | ProtocolError::ListenerNotFound
        | ProtocolError::OffsetOutOfRange
        | ProtocolError::PositionOutOfRange
        | ProtocolError::InvalidPrincipalType => Classified::Backend,
        _ => Classified::Backend,
    }
}

/// 文本启发式分类（顺序敏感：先匹配更具体的模式）。
fn classify_message(message: &str) -> Classified {
    let text = message.to_ascii_lowercase();
    // 先判不可重试的具体模式，避免 `unknown topic id` 被 `unknown topic` 规则误判为可重试。
    if text.contains("too large")
        || text.contains("toolarge")
        || text.contains("too_large")
        || text.contains("message_size")
        || text.contains("record_too_large")
        || text.contains("unknown_topic_id")
        || text.contains("unknown topic id")
        || text.contains("invalid topic")
        || text.contains("does not exist")
        || text.contains("not_exist")
        || text.contains("not found")
        || text.contains("already exists")
    {
        return Classified::Backend;
    }
    if text.contains("timeout") || text.contains("timed out") {
        return Classified::Timeout;
    }
    if text.contains("cancel") {
        return Classified::Closed;
    }
    if text.contains("unknown_topic")
        || text.contains("unknown topic")
        || text.contains("not leader")
        || text.contains("leader")
        || text.contains("retriable")
        || text.contains("rebalance")
        || text.contains("network")
        || text.contains("coordinator")
    {
        return Classified::Transient;
    }
    if text.contains("auth")
        || text.contains("sasl")
        || text.contains("ssl")
        || text.contains("tls")
        || text.contains("permission")
        || text.contains("invalid")
        || text.contains("unsupported")
    {
        return Classified::Backend;
    }
    Classified::Connection
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_timeout_text_to_timeout() {
        let error = map_kafka_error("produce", std::io::Error::other("request timed out"));
        assert!(matches!(error, KafkaError::Timeout(_)));
        assert!(error.is_retryable());
    }

    #[test]
    fn maps_connection_text_to_connection() {
        let error = map_kafka_error("connect", std::io::Error::other("Connection refused"));
        assert!(matches!(error, KafkaError::Connection(_)));
        assert!(error.is_retryable());
    }

    #[test]
    fn maps_message_too_large_to_backend_non_retryable() {
        let error = map_kafka_error("produce", std::io::Error::other("MessageTooLarge"));
        assert!(matches!(error, KafkaError::Backend(_)));
        assert!(!error.is_retryable());
    }

    #[test]
    fn unknown_topic_or_partition_is_retryable_but_unknown_topic_id_is_not() {
        let retryable =
            map_kafka_error("fetch", std::io::Error::other("Unknown topic or partition"));
        assert_eq!(retryable.kind(), "transient");
        assert!(retryable.is_retryable());

        let permanent = map_kafka_error("fetch", std::io::Error::other("UNKNOWN_TOPIC_ID"));
        assert_eq!(permanent.kind(), "backend");
        assert!(!permanent.is_retryable());
    }

    #[test]
    fn leader_change_is_retryable() {
        let error = map_kafka_error("produce", std::io::Error::other("NotLeaderOrFollower"));
        assert!(matches!(error, KafkaError::Transient(_)));
        assert!(error.is_retryable());
    }

    #[test]
    fn public_message_does_not_echo_driver_text() {
        let error = map_kafka_error("connect", std::io::Error::other("host=10.0.0.1 secret"));
        assert!(!error.to_string().contains("10.0.0.1"));
        assert!(!error.to_string().contains("secret"));
        assert!(error.to_string().contains("connect"));
    }

    #[test]
    fn protocol_codes_classify_structurally() {
        assert_eq!(
            classify_protocol(ProtocolError::NotLeaderOrFollower),
            Classified::Transient
        );
        assert_eq!(
            classify_protocol(ProtocolError::UnknownTopicOrPartition),
            Classified::Transient
        );
        assert_eq!(
            classify_protocol(ProtocolError::MessageTooLarge),
            Classified::Backend
        );
        assert_eq!(
            classify_protocol(ProtocolError::UnknownTopicId),
            Classified::Backend
        );
        assert_eq!(
            classify_protocol(ProtocolError::SaslAuthenticationFailed),
            Classified::Backend
        );
        assert_eq!(
            classify_protocol(ProtocolError::RequestTimedOut),
            Classified::Transient
        );
    }
}
