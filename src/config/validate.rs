//! 配置校验与 broker host 解析。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。`broker_host` / `host_is_loopback`
//! 只被本模块的 `validate` 调用，**保持私有**；`redact_brokers` 留给门面的 `Debug` 实现，
//! 故未下沉。

use super::*;

impl KafkaConfig {
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
