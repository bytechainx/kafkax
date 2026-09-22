//! 环境变量加载层：`from_env`、`apply_env_overlay` 与两个解析辅助。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。搬走的项除两个 `pub` 方法外
//! 全是私有且只在本层内互调，故**无需任何可见性调整**。

use super::*;

impl KafkaConfig {
    /// 从环境变量加载（缺省回落 [`KafkaConfig::default`]）。
    ///
    /// # Errors
    ///
    /// 环境变量取值非法或校验失败时返回 [`KafkaError::Config`]。
    pub fn from_env() -> KafkaResult<Self> {
        let mut config = Self::default();
        config.apply_env_overlay()?;
        config.validate()?;
        Ok(config)
    }

    /// 用环境变量覆盖当前值。
    fn apply_env_overlay(&mut self) -> KafkaResult<()> {
        if let Ok(value) = std::env::var(ENV_BROKERS) {
            if !value.trim().is_empty() {
                self.brokers = value;
            }
        }
        if let Ok(value) = std::env::var(ENV_CLIENT_ID) {
            if !value.trim().is_empty() {
                self.client_id = value;
            }
        }
        if let Ok(value) = std::env::var(ENV_SASL_MECHANISM) {
            let mechanism = value.trim();
            if mechanism.is_empty() || mechanism.eq_ignore_ascii_case("none") {
                self.sasl_mechanism = None;
                self.sasl_username = None;
                self.sasl_password = None;
            } else {
                self.sasl_mechanism = Some(mechanism.to_string());
            }
        }
        if let Ok(value) = std::env::var(ENV_SASL_USERNAME) {
            let username = value.trim();
            self.sasl_username = if username.is_empty() {
                None
            } else {
                Some(username.into())
            };
        }
        if let Ok(value) = std::env::var(ENV_SASL_PASSWORD) {
            let password = value.trim();
            self.sasl_password = if password.is_empty() {
                None
            } else {
                Some(password.into())
            };
        }
        if let Ok(value) = std::env::var(ENV_TLS) {
            self.tls = parse_bool(&value, ENV_TLS)?;
        }
        if let Ok(value) = std::env::var(ENV_TLS_CA_FILE) {
            if !value.trim().is_empty() {
                self.tls_ca_file = Some(PathBuf::from(value));
            }
        }
        if let Ok(value) = std::env::var(ENV_CONNECT_TIMEOUT_MS) {
            self.connect_timeout = parse_millis(&value, ENV_CONNECT_TIMEOUT_MS)?;
        }
        if let Ok(value) = std::env::var(ENV_OPERATION_TIMEOUT_MS) {
            self.operation_timeout = parse_millis(&value, ENV_OPERATION_TIMEOUT_MS)?;
        }
        if let Ok(value) = std::env::var(ENV_DELIVERY_TIMEOUT_MS) {
            self.delivery_timeout = parse_millis(&value, ENV_DELIVERY_TIMEOUT_MS)?;
        }
        Ok(())
    }
}

/// 解析布尔型环境变量。
fn parse_bool(value: &str, name: &str) -> KafkaResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(KafkaError::Config(format!("{name} 非法: {value}"))),
    }
}

/// 解析毫秒型环境变量。
fn parse_millis(value: &str, name: &str) -> KafkaResult<Duration> {
    value
        .trim()
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|error| KafkaError::Config(format!("{name} 非法: {error}")))
}
