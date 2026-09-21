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
    /// 从 TOML 字符串解析并校验。
    ///
    /// 字段名与结构体字段一致；时长可写 `{ secs = 30 }` / `{ secs = 1, nanos = 500 }`
    /// 或整数毫秒（`connect_timeout = 1500`）。
    /// 为杜绝凭据入库，TOML 中的 `sasl_username` / `sasl_password` 会被显式拒绝，
    /// 凭据只能经 `FOUNDATIONX_KAFKAX_SASL_*` 或构建器注入。
    ///
    /// # Errors
    ///
    /// TOML 语法错误、含未知字段、含凭据字段或校验失败时返回 [`KafkaError::Config`]。
    /// 错误消息只含错误摘要与位置（行号 + 字节区间），**不回显 TOML 源码**，
    /// 以免出错行承载凭据时把凭据片段带进日志。
    pub fn from_toml(text: &str) -> KafkaResult<Self> {
        reject_secret_keys_in_toml(text)?;
        let config: Self = toml::from_str(text).map_err(|error| {
            KafkaError::Config(format!(
                "TOML 配置非法: {}",
                toml_error_summary(text, &error)
            ))
        })?;
        config.validate()?;
        Ok(config)
    }

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

    /// 校验配置合法性。
    ///
    /// # Errors
    ///
    /// 以下情况返回 [`KafkaError::Config`]：brokers 为空或全为分隔符、地址缺少 host 或内嵌
    /// userinfo、超时为零、配置了 CA 文件但未启用 TLS、远程 broker 未启用 TLS、非 `PLAIN`
    /// 的 SASL 机制、SASL 凭据缺失/多余、`client_id` 为空。
    pub fn validate(&self) -> KafkaResult<()> {
        if self.brokers.trim().is_empty() {
            return Err(KafkaError::Config("brokers 不能为空".into()));
        }
        if self.delivery_timeout.is_zero()
            || self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
        {
            return Err(KafkaError::Config("timeout 必须大于零".into()));
        }
        if self.tls_ca_file.is_some() && !self.tls {
            return Err(KafkaError::Config("配置 tls_ca_file 时必须启用 TLS".into()));
        }
        let brokers: Vec<&str> = self
            .brokers
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .collect();
        if brokers.is_empty() {
            return Err(KafkaError::Config("brokers 至少包含一个有效地址".into()));
        }
        for broker in brokers {
            let host = broker_host(broker)?;
            if !self.tls && !host_is_loopback(&host) {
                return Err(KafkaError::Config(format!(
                    "远程 broker `{host}` 必须启用 TLS"
                )));
            }
        }
        if let Some(mechanism) = &self.sasl_mechanism {
            if !mechanism.eq_ignore_ascii_case(DEFAULT_SASL_MECHANISM) {
                return Err(KafkaError::Config(format!(
                    "当前仅支持 SASL/PLAIN，拒绝机制 `{mechanism}`"
                )));
            }
            if self.sasl_username.as_deref().unwrap_or_default().is_empty() {
                return Err(KafkaError::Config("已启用 SASL 但缺少 username".into()));
            }
            if self.sasl_password.as_deref().unwrap_or_default().is_empty() {
                return Err(KafkaError::Config("已启用 SASL 但缺少 password".into()));
            }
        } else if self.sasl_username.is_some() || self.sasl_password.is_some() {
            return Err(KafkaError::Config(
                "提供了 SASL 凭据但未启用 PLAIN 机制".into(),
            ));
        }
        if self.client_id.trim().is_empty() {
            return Err(KafkaError::Config("client_id 不能为空".into()));
        }
        Ok(())
    }

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

/// [`KafkaConfig`] 的链式构建器；校验发生在 [`KafkaConfigBuilder::build`]。
#[derive(Clone, Debug, Default)]
pub struct KafkaConfigBuilder {
    inner: KafkaConfig,
}

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

/// 解析 broker 的 host，拒绝内嵌 userinfo 与缺失 host 的地址。
fn broker_host(broker: &str) -> KafkaResult<String> {
    let candidate = if broker.contains("://") {
        broker.to_string()
    } else {
        format!("kafka://{broker}")
    };
    let parsed = url::Url::parse(&candidate)
        .map_err(|error| KafkaError::Config(format!("broker 地址非法: {error}")))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(KafkaError::Config("broker 地址禁止内嵌 userinfo".into()));
    }
    parsed
        .host_str()
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| KafkaError::Config("broker 缺少 host".into()))
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

/// 判断 host 是否为本机回环地址。
fn host_is_loopback(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
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

/// 渲染 TOML 错误摘要：只保留错误消息与位置，**不带源码片段**。
///
/// `toml` 的错误 `Display` 会把出错行的原始源码一起渲染。当出错行正是承载凭据的那一行
/// （例如 `sasl_password = "…` 引号未闭合），凭据片段就会随公开错误消息进入日志与打点，
/// 违反「错误消息不得泄漏敏感值」的安全基线与标准.md §2 的凭据治理要求。
/// 因此这里退化为「消息 + 行号 + 字节区间」：保留可定位性，不回显输入内容。
fn toml_error_summary(text: &str, error: &toml::de::Error) -> String {
    let Some(span) = error.span() else {
        return error.message().to_string();
    };
    let line = text
        .get(..span.start)
        .map_or(1, |head| head.matches('\n').count() + 1);
    format!(
        "{}（第 {line} 行，字节区间 {}..{}）",
        error.message(),
        span.start,
        span.end
    )
}

/// 拒绝 TOML 中的凭据字段，避免明文入库。
fn reject_secret_keys_in_toml(text: &str) -> KafkaResult<()> {
    let value: toml::Value = toml::from_str(text).map_err(|error| {
        KafkaError::Config(format!(
            "TOML 解析失败: {}",
            toml_error_summary(text, &error)
        ))
    })?;
    let Some(table) = value.as_table() else {
        return Err(KafkaError::Config("TOML 根必须为表".into()));
    };
    for key in ["sasl_password", "sasl_username"] {
        if table.contains_key(key) {
            return Err(KafkaError::Config(format!("TOML 禁止字段 {key}")));
        }
    }
    Ok(())
}

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
