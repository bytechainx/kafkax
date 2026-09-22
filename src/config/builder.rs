//! [`KafkaConfigBuilder`] 的链式构建器实现。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。搬走的 11 个方法全是 `pub`
//! （`new` 在内），故**无需任何可见性调整**；结构体本身留在门面。

use super::*;

impl KafkaConfigBuilder {
    /// 从默认值开始。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: KafkaConfig::default(),
        }
    }

    /// 设置 `bootstrap.servers`。
    #[must_use]
    pub fn brokers(mut self, brokers: impl Into<String>) -> Self {
        self.inner.brokers = brokers.into();
        self
    }

    /// 设置 `client.id`。
    #[must_use]
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.inner.client_id = client_id.into();
        self
    }

    /// 启用 SASL/PLAIN 并设置凭据。
    #[must_use]
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.inner.sasl_mechanism = Some(DEFAULT_SASL_MECHANISM.to_string());
        self.inner.sasl_username = Some(username.into());
        self.inner.sasl_password = Some(password.into());
        self
    }

    /// 关闭 SASL 并清除凭据。
    #[must_use]
    pub fn no_sasl(mut self) -> Self {
        self.inner.sasl_mechanism = None;
        self.inner.sasl_username = None;
        self.inner.sasl_password = None;
        self
    }

    /// 启用/关闭 TLS。
    #[must_use]
    pub fn tls(mut self, enable: bool) -> Self {
        self.inner.tls = enable;
        self
    }

    /// 指定 PEM CA 文件（同时要求 [`Self::tls`]）。
    #[must_use]
    pub fn tls_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.tls_ca_file = Some(path.into());
        self
    }

    /// 设置 produce 投递截止时间。
    #[must_use]
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.inner.delivery_timeout = timeout;
        self
    }

    /// 设置建连截止时间。
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.inner.connect_timeout = timeout;
        self
    }

    /// 设置元数据/管理操作截止时间。
    #[must_use]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.inner.operation_timeout = timeout;
        self
    }

    /// 校验并产出配置。
    ///
    /// # Errors
    ///
    /// 与 [`KafkaConfig::validate`] 相同。
    pub fn build(self) -> KafkaResult<KafkaConfig> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}
