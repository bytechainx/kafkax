//! Kafka 连接配置：默认值、环境变量与 TOML。
//!
//! ## 覆盖顺序（低 → 高）
//!
//! 1. 代码默认值（[`KafkaConfig::default`]）
//! 2. TOML（[`KafkaConfig::from_toml`]）
//! 3. 环境变量 `FOUNDATIONX_KAFKAX_*`（[`KafkaConfig::from_env`]）
//! 4. [`KafkaConfigBuilder`] 显式构建
//!
//! ## 环境变量（前缀 `FOUNDATIONX_KAFKAX_`）
//!
//! 常量见 [`ENV_BROKERS`]、[`ENV_SASL_MECHANISM`]、[`ENV_SASL_USERNAME`]、
//! [`ENV_SASL_PASSWORD`]、[`ENV_TLS`]、[`ENV_TLS_CA_FILE`]、[`ENV_CLIENT_ID`]、
//! [`ENV_CONNECT_TIMEOUT_MS`]、[`ENV_OPERATION_TIMEOUT_MS`]、[`ENV_DELIVERY_TIMEOUT_MS`]。
//!
//! 默认值面向本地联调（`127.0.0.1:9092`、明文、无凭据）。生产环境必须经环境变量注入凭据，
//! 且远程 broker 在未启用 TLS 时会被 [`KafkaConfig::validate`] 拒绝（fail-closed）。
//! `sasl_password` 与 `sasl_username` 的 `Debug` 输出已脱敏。

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{KafkaError, KafkaResult};

/// 默认 bootstrap 地址。
pub const DEFAULT_BROKERS: &str = "127.0.0.1:9092";
/// 默认 SASL 机制名（仅 `PLAIN` 受支持）。
pub const DEFAULT_SASL_MECHANISM: &str = "PLAIN";

/// `FOUNDATIONX_KAFKAX_BROKERS`：bootstrap servers（逗号分隔）。
pub const ENV_BROKERS: &str = "FOUNDATIONX_KAFKAX_BROKERS";
/// `FOUNDATIONX_KAFKAX_CLIENT_ID`：`client.id`。
pub const ENV_CLIENT_ID: &str = "FOUNDATIONX_KAFKAX_CLIENT_ID";
/// `FOUNDATIONX_KAFKAX_SASL_MECHANISM`：SASL 机制；空串或 `none` 关闭 SASL。
pub const ENV_SASL_MECHANISM: &str = "FOUNDATIONX_KAFKAX_SASL_MECHANISM";
/// `FOUNDATIONX_KAFKAX_SASL_USERNAME`：SASL 用户名。
pub const ENV_SASL_USERNAME: &str = "FOUNDATIONX_KAFKAX_SASL_USERNAME";
/// `FOUNDATIONX_KAFKAX_SASL_PASSWORD`：SASL 密码（Debug 脱敏，不回显）。
pub const ENV_SASL_PASSWORD: &str = "FOUNDATIONX_KAFKAX_SASL_PASSWORD";
/// `FOUNDATIONX_KAFKAX_TLS`：`1`/`true`/`yes`/`on` 开启 TLS。
pub const ENV_TLS: &str = "FOUNDATIONX_KAFKAX_TLS";
/// `FOUNDATIONX_KAFKAX_TLS_CA_FILE`：自定义 PEM CA 文件路径。
pub const ENV_TLS_CA_FILE: &str = "FOUNDATIONX_KAFKAX_TLS_CA_FILE";
/// `FOUNDATIONX_KAFKAX_CONNECT_TIMEOUT_MS`：建连截止时间（毫秒）。
pub const ENV_CONNECT_TIMEOUT_MS: &str = "FOUNDATIONX_KAFKAX_CONNECT_TIMEOUT_MS";
/// `FOUNDATIONX_KAFKAX_OPERATION_TIMEOUT_MS`：元数据/控制面操作截止时间（毫秒）。
pub const ENV_OPERATION_TIMEOUT_MS: &str = "FOUNDATIONX_KAFKAX_OPERATION_TIMEOUT_MS";
/// `FOUNDATIONX_KAFKAX_DELIVERY_TIMEOUT_MS`：produce 投递确认截止时间（毫秒）。
pub const ENV_DELIVERY_TIMEOUT_MS: &str = "FOUNDATIONX_KAFKAX_DELIVERY_TIMEOUT_MS";

/// Kafka 客户端配置。
///
/// 可直接由 TOML/JSON 反序列化（自定义 `Debug` 会脱敏凭据）。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaConfig {
    /// `bootstrap.servers`（逗号分隔）。
    pub brokers: String,
    /// `client.id`。
    pub client_id: String,
    /// SASL 机制；`None` 表示无认证明文连接。
    pub sasl_mechanism: Option<String>,
    /// SASL 用户名（`Debug` 脱敏）。
    sasl_username: Option<String>,
    /// SASL 密码（`Debug` 脱敏）。
    sasl_password: Option<String>,
    /// 是否启用 TLS（rustls）。
    pub tls: bool,
    /// 自定义 PEM CA 文件；`None` 时使用 `webpki-roots` 公共根证书。
    pub tls_ca_file: Option<PathBuf>,
    /// produce 等待 broker 确认的截止时间。
    #[serde(deserialize_with = "de_duration")]
    pub delivery_timeout: Duration,
    /// 建连截止时间。
    #[serde(deserialize_with = "de_duration")]
    pub connect_timeout: Duration,
    /// 元数据与管理操作截止时间。
    #[serde(deserialize_with = "de_duration")]
    pub operation_timeout: Duration,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: DEFAULT_BROKERS.to_string(),
            client_id: "kafkax".to_string(),
            // 不内置任何真实/草稿凭据
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            tls: false,
            tls_ca_file: None,
            delivery_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(10),
        }
    }
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &redact_brokers(&self.brokers))
            .field("client_id", &self.client_id)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username.as_ref().map(|_| "***"))
            .field("sasl_password", &self.sasl_password.as_ref().map(|_| "***"))
            .field("tls", &self.tls)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("delivery_timeout", &self.delivery_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl KafkaConfig {
    /// 安全协议字符串：`PLAINTEXT` / `SASL_PLAINTEXT` / `SSL` / `SASL_SSL`。
    #[must_use]
    pub fn security_protocol(&self) -> &'static str {
        match (self.tls, self.sasl_mechanism.is_some()) {
            (true, true) => "SASL_SSL",
            (true, false) => "SSL",
            (false, true) => "SASL_PLAINTEXT",
            (false, false) => "PLAINTEXT",
        }
    }

    /// 创建构建器（从默认值开始）。
    #[must_use]
    pub fn builder() -> KafkaConfigBuilder {
        KafkaConfigBuilder::new()
    }

    /// 返回 SASL 凭据；仅供 crate 内连接实现使用。
    pub(crate) fn sasl_credentials(&self) -> Option<(&str, &str)> {
        match (self.sasl_username.as_deref(), self.sasl_password.as_deref()) {
            (Some(username), Some(password)) => Some((username, password)),
            _ => None,
        }
    }
}

/// [`KafkaConfig`] 的链式构建器；校验发生在 [`KafkaConfigBuilder::build`]。
#[derive(Clone, Debug, Default)]
pub struct KafkaConfigBuilder {
    inner: KafkaConfig,
}

/// 时长反序列化：支持 `{ secs = 30 }`（`nanos` 可省）或整数毫秒。
///
/// 例：`delivery_timeout = { secs = 5 }`、`connect_timeout = 1500`（毫秒）。
fn de_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    /// TOML 中允许的两种时长写法。
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawDuration {
        /// 显式秒/纳秒表。
        Table {
            secs: u64,
            #[serde(default)]
            nanos: u32,
        },
        /// 整数毫秒。
        Millis(u64),
    }

    match RawDuration::deserialize(deserializer)? {
        RawDuration::Table { secs, nanos } => {
            if nanos > 999_999_999 {
                return Err(serde::de::Error::custom("nanos 必须在 0..=999999999 之间"));
            }
            Duration::from_secs(secs)
                .checked_add(Duration::from_nanos(u64::from(nanos)))
                .ok_or_else(|| serde::de::Error::custom("时长超出 Duration 表示范围"))
        }
        RawDuration::Millis(millis) => Ok(Duration::from_millis(millis)),
    }
}

/// `Debug` 输出时脱敏 broker userinfo。
fn redact_brokers(brokers: &str) -> String {
    brokers
        .split(',')
        .map(|broker| {
            if broker.contains('@') {
                "<redacted-userinfo>"
            } else {
                broker.trim()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

mod builder;
mod envvars;
mod tomlfile;
mod validate;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid_and_has_no_credentials() {
        let config = KafkaConfig::default();
        assert_eq!(config.brokers, DEFAULT_BROKERS);
        assert!(config.sasl_mechanism.is_none());
        assert!(config.sasl_username.is_none());
        assert!(config.sasl_password.is_none());
        assert!(!config.tls);
        config.validate().expect("默认配置合法");
    }

    #[test]
    fn debug_redacts_credentials_and_userinfo() {
        let config = KafkaConfig {
            brokers: "kafka://embedded:secret@localhost:9092".into(),
            ..KafkaConfig::builder()
                .sasl_plain("admin", "super-secret")
                .build()
                .expect("合法")
        };
        let text = format!("{config:?}");
        assert!(text.contains("***"));
        assert!(!text.contains("super-secret"));
        assert!(!text.contains("admin"));
        assert!(!text.contains("embedded"));
    }

    #[test]
    fn remote_plaintext_is_rejected_but_remote_tls_is_accepted() {
        let plain = KafkaConfig {
            brokers: "broker.example.com:9092".into(),
            ..KafkaConfig::default()
        };
        assert!(plain.validate().is_err());

        let tls = KafkaConfig {
            brokers: "broker.example.com:9093".into(),
            tls: true,
            ..KafkaConfig::default()
        };
        tls.validate().expect("远程 TLS 配置合法");
    }

    #[test]
    fn only_plain_sasl_is_accepted() {
        let config = KafkaConfig::builder()
            .sasl_plain("user", "secret")
            .build()
            .map(|mut config| {
                config.sasl_mechanism = Some("SCRAM-SHA-256".into());
                config
            })
            .expect("loopback 合法");
        assert!(config.validate().is_err());
    }

    #[test]
    fn toml_roundtrip_and_secret_rejection() {
        let config = KafkaConfig::from_toml(
            r#"
brokers = "127.0.0.1:9092"
client_id = "demo"
delivery_timeout = { secs = 5 }
operation_timeout = { secs = 1, nanos = 500000000 }
connect_timeout = 1500
"#,
        )
        .expect("合法 TOML");
        assert_eq!(config.client_id, "demo");
        assert_eq!(config.delivery_timeout, Duration::from_secs(5));
        assert_eq!(config.operation_timeout, Duration::from_millis(1500));
        assert_eq!(config.connect_timeout, Duration::from_millis(1500));

        let secret = "brokers = \"127.0.0.1:9092\"\nsasl_password = \"secret\"\n";
        let error = KafkaConfig::from_toml(secret).expect_err("凭据字段必须被拒绝");
        assert!(!error.to_string().contains("secret"));

        let unknown = "brokers = \"127.0.0.1:9092\"\nsink_id = \"x\"\n";
        assert!(KafkaConfig::from_toml(unknown).is_err());
    }

    #[test]
    fn ipv6_loopback_and_separator_only_brokers() {
        let ipv6 = KafkaConfig {
            brokers: "[::1]:9092".into(),
            ..KafkaConfig::default()
        };
        ipv6.validate().expect("IPv6 回环允许明文");

        let separators = KafkaConfig {
            brokers: " , , ".into(),
            ..KafkaConfig::default()
        };
        assert!(separators.validate().is_err());
    }
}
