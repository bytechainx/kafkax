//! kafkax 统一错误类型。
//!
//! 所有公开入口都返回 [`KafkaResult`]；[`KafkaError::is_retryable`] 给出可重试判定，
//! 供调用方实现自己的退避策略（本库不做隐式重试）。

/// kafkax 错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KafkaError {
    /// 配置非法（本地校验失败、环境变量/TLS 文件缺失等）。
    #[error("配置无效: {0}")]
    Config(String),
    /// 连接建立或维护失败（含网络不可达、TLS/SASL 握手失败）。
    #[error("连接失败: {0}")]
    Connection(String),
    /// 远端返回业务/协议错误，且该请求本身不可重试（如消息过大、主题非法）。
    #[error("远端返回错误: {0}")]
    Backend(String),
    /// 远端瞬时故障（leader 变更、分区元数据未就绪、可重试标记等）。
    #[error("远端瞬时故障: {0}")]
    Transient(String),
    /// 序列化或解析失败。
    #[error("序列化失败: {0}")]
    Serialization(String),
    /// 网络或本地 I/O 失败。
    #[error("I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// 操作超时。
    #[error("操作超时: {0}")]
    Timeout(String),
    /// 当前能力不支持（例如需要 consumer group / 事务 / schema registry 的操作）。
    #[error("不支持的操作: {0}")]
    Unsupported(String),
    /// 连接池已关闭，或操作被关闭信号取消。
    #[error("连接池已关闭: {0}")]
    Closed(String),
}

impl KafkaError {
    /// 是否属于可安全重试的瞬时错误。
    ///
    /// 可重试：[`KafkaError::Connection`]、[`KafkaError::Transient`]、[`KafkaError::Timeout`]。
    /// 不可重试：配置/序列化/本地 I/O/能力缺失/连接池已关闭，以及被远端明确拒绝的
    /// [`KafkaError::Backend`]。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(_) | Self::Transient(_) | Self::Timeout(_) => true,
            Self::Config(_)
            | Self::Backend(_)
            | Self::Serialization(_)
            | Self::Io(_)
            | Self::Unsupported(_)
            | Self::Closed(_) => false,
        }
    }

    /// 错误分类名（低基数，便于打点与日志聚合）。
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Connection(_) => "connection",
            Self::Backend(_) => "backend",
            Self::Transient(_) => "transient",
            Self::Serialization(_) => "serialization",
            Self::Io(_) => "io",
            Self::Timeout(_) => "timeout",
            Self::Unsupported(_) => "unsupported",
            Self::Closed(_) => "closed",
        }
    }
}

/// crate 专用 `Result` 别名。
pub type KafkaResult<T> = Result<T, KafkaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification_is_exhaustive() {
        let retryable = [
            KafkaError::Connection("x".into()),
            KafkaError::Transient("x".into()),
            KafkaError::Timeout("x".into()),
        ];
        let permanent = [
            KafkaError::Config("x".into()),
            KafkaError::Backend("x".into()),
            KafkaError::Serialization("x".into()),
            KafkaError::Io(std::io::Error::other("x")),
            KafkaError::Unsupported("x".into()),
            KafkaError::Closed("x".into()),
        ];
        for error in retryable {
            assert!(error.is_retryable(), "{} 应可重试", error.kind());
        }
        for error in permanent {
            assert!(!error.is_retryable(), "{} 不应重试", error.kind());
        }
    }

    #[test]
    fn io_error_converts_with_from() {
        let error: KafkaError = std::io::Error::other("disk").into();
        assert_eq!(error.kind(), "io");
    }
}
