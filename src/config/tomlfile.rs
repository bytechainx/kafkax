//! TOML 解析层：`from_toml` 与两个辅助（错误摘要 / 凭据字段拒绝）。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。子模块名用 `tomlfile` 而非 `toml`，
//! 避免 edition 2018 的 uniform path 让本地模块遮蔽 `toml` 依赖 crate。
//! `de_duration`（`#[serde(deserialize_with = …)]` 的目标）刻意留在门面：该路径按
//! **结构体所在模块**解析，随结构体留在一起最稳，且它只有 33 行。

use super::*;

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
