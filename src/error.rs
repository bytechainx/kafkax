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
    /// 可重试：[`KafkaError::Connection`]、[`KafkaError::Transient`]、[`KafkaError::Timeout`]，
    /// 以及 I/O 错误中属于瞬时类的子集（超时、中断、磁盘暂时满等）。
    /// 不可重试：配置/序列化/永久性 I/O（文件不存在、权限不足等）/能力缺失/连接池已关闭，
    /// 以及被远端明确拒绝的 [`KafkaError::Backend`]。
    ///
    /// I/O 可重试的错误类型覆盖了 `FileOffsetStore::commit()` 路径中最常见的瞬态磁盘
    /// 故障（ENOSPC、EAGAIN、EINTR 等），确保 at-least-once consumer 在 offset 提交
    /// 遭遇短暂磁盘压力后可通过重试自动恢复。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(_) | Self::Transient(_) | Self::Timeout(_) => true,
            Self::Config(_)
            | Self::Backend(_)
            | Self::Serialization(_)
            | Self::Unsupported(_)
            | Self::Closed(_) => false,
            Self::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::WriteZero
                    | std::io::ErrorKind::StorageFull
            ),
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
        // Io 中的永久性错误仍不可重试
        let permanent = [
            KafkaError::Config("x".into()),
            KafkaError::Backend("x".into()),
            KafkaError::Serialization("x".into()),
            KafkaError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "file missing",
            )),
            KafkaError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "access denied",
            )),
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
    fn io_transient_errors_are_retryable() {
        // 瞬态 I/O 错误应可重试——这些是 FileOffsetStore::commit()
        // 路径中最常见的磁盘故障类型（ENOSPC、EAGAIN、EINTR 等）
        let transient_kinds = [
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::WriteZero,
            std::io::ErrorKind::StorageFull,
        ];
        for kind in transient_kinds {
            let error = KafkaError::Io(std::io::Error::new(kind, "simulated"));
            assert!(
                error.is_retryable(),
                "Io({kind:?}) 应可重试——offset commit 路径的瞬态磁盘故障需自动恢复"
            );
        }
    }

    #[test]
    fn io_permanent_errors_are_not_retryable() {
        // 永久性 I/O 错误不应重试（文件不存在、权限不足等）
        let permanent_kinds = [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists,
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::InvalidData,
        ];
        for kind in permanent_kinds {
            let error = KafkaError::Io(std::io::Error::new(kind, "simulated"));
            assert!(
                !error.is_retryable(),
                "Io({kind:?}) 不应重试——永久性错误重试无意义"
            );
        }
    }

    #[test]
    fn io_error_converts_with_from() {
        let error: KafkaError = std::io::Error::other("disk").into();
        assert_eq!(error.kind(), "io");
    }
}
