#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 配置校验、环境变量覆盖、安全协议与凭据脱敏。

use std::time::Duration;

use kafkax::{
    KafkaConfig, KafkaError, DEFAULT_BROKERS, DEFAULT_SASL_MECHANISM, ENV_BROKERS, ENV_CLIENT_ID,
    ENV_CONNECT_TIMEOUT_MS, ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS, ENV_SASL_MECHANISM,
    ENV_SASL_PASSWORD, ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};

#[test]
fn defaults_are_local_and_credential_free() {
    let config = KafkaConfig::default();
    assert_eq!(config.brokers, DEFAULT_BROKERS);
    assert_eq!(DEFAULT_SASL_MECHANISM, "PLAIN");
    assert_eq!(config.security_protocol(), "PLAINTEXT");
    assert!(config.sasl_mechanism.is_none());
    assert!(!config.tls);
    assert_eq!(config.delivery_timeout, Duration::from_secs(30));
    config.validate().expect("默认配置合法");
}

#[test]
fn security_protocol_matrix() {
    let plaintext = KafkaConfig::default();
    assert_eq!(plaintext.security_protocol(), "PLAINTEXT");

    let sasl = KafkaConfig::builder()
        .sasl_plain("user", "password")
        .build()
        .expect("回环 + PLAIN 合法");
    assert_eq!(sasl.security_protocol(), "SASL_PLAINTEXT");

    let sasl_tls = KafkaConfig::builder()
        .brokers("broker.example.com:9093")
        .sasl_plain("user", "password")
        .tls(true)
        .build()
        .expect("远程 TLS + PLAIN 合法");
    assert_eq!(sasl_tls.security_protocol(), "SASL_SSL");

    let tls = KafkaConfig::builder()
        .brokers("broker.example.com:9093")
        .tls(true)
        .build()
        .expect("远程 TLS 合法");
    assert_eq!(tls.security_protocol(), "SSL");
}

/// 基于默认值构造配置，仅改公开字段（凭据字段是私有的，只能走构建器）。
fn config_with(mutate: impl FnOnce(&mut KafkaConfig)) -> KafkaConfig {
    let mut config = KafkaConfig::default();
    mutate(&mut config);
    config
}

#[test]
fn validate_rejects_invalid_shapes() {
    let cases: [(&str, KafkaConfig); 6] = [
        (
            "空 brokers",
            config_with(|config| config.brokers = "  ".into()),
        ),
        (
            "全分隔符 brokers",
            config_with(|config| config.brokers = " , , ".into()),
        ),
        (
            "零超时",
            config_with(|config| config.connect_timeout = Duration::ZERO),
        ),
        (
            "零投递超时",
            config_with(|config| config.delivery_timeout = Duration::ZERO),
        ),
        (
            "空 client_id",
            config_with(|config| config.client_id = " ".into()),
        ),
        (
            "CA 未启用 TLS",
            config_with(|config| config.tls_ca_file = Some("/tmp/ca.pem".into())),
        ),
    ];
    for (name, config) in cases {
        let error = config.validate().expect_err(name);
        assert!(
            matches!(error, KafkaError::Config(_)),
            "{name} 应返回 Config 错误"
        );
        assert!(!error.is_retryable(), "{name} 不应被标记为可重试");
    }
}

#[test]
fn validate_accepts_loopback_forms_and_rejects_userinfo() {
    config_with(|config| config.brokers = "127.0.0.1:9092, localhost:9093".into())
        .validate()
        .expect("多个回环地址合法");
    config_with(|config| config.brokers = "[::1]:9092".into())
        .validate()
        .expect("IPv6 回环合法");
    config_with(|config| config.brokers = "broker.example.com:9093".into())
        .validate()
        .expect_err("远程必须 TLS");

    let userinfo = config_with(|config| config.brokers = "kafka://user:pass@127.0.0.1:9092".into());
    assert!(userinfo.validate().is_err(), "禁止内嵌 userinfo");
}

#[test]
fn validate_rejects_unsupported_sasl_mechanisms() {
    let mut unknown = KafkaConfig::builder()
        .sasl_plain("user", "password")
        .build()
        .expect("合法");
    unknown.sasl_mechanism = Some("SCRAM-SHA-256".into());
    assert!(unknown.validate().is_err(), "仅支持 PLAIN");

    // 构建器置空机制：凭据仍在，必须 fail-closed
    let credentials_without_mechanism = KafkaConfig::builder()
        .sasl_plain("user", "password")
        .build()
        .expect("合法")
        .tap(|config| config.sasl_mechanism = None);
    assert!(
        credentials_without_mechanism.validate().is_err(),
        "凭据不得被静默忽略"
    );
}

/// 链式修改辅助（测试内部使用）。
trait Tap: Sized {
    fn tap(mut self, mutate: impl FnOnce(&mut Self)) -> Self {
        mutate(&mut self);
        self
    }
}

impl Tap for KafkaConfig {}

#[test]
fn debug_redacts_password_username_and_broker_userinfo() {
    let mut config = KafkaConfig::builder()
        .sasl_plain("admin", "super-secret-kafka")
        .build()
        .expect("回环配置合法");
    // 直接改 pub 字段：Debug 脱敏不依赖 validate 结果
    config.brokers = "kafka://embedded:secret@localhost:9092".into();
    let text = format!("{config:?}");
    assert!(text.contains("***"), "应包含脱敏占位: {text}");
    assert!(!text.contains("super-secret-kafka"));
    assert!(!text.contains("admin"));
    assert!(!text.contains("embedded"));
    assert!(!text.contains("secret@"));
}

#[test]
fn builder_reports_validation_errors() {
    let error = KafkaConfig::builder()
        .brokers("broker.example.com:9092")
        .build()
        .expect_err("远程明文必须 fail-closed");
    assert!(matches!(error, KafkaError::Config(_)));

    let config = KafkaConfig::builder()
        .brokers("127.0.0.1:9092")
        .client_id("builder")
        .no_sasl()
        .tls(false)
        .operation_timeout(Duration::from_millis(250))
        .build()
        .expect("合法配置");
    assert_eq!(config.client_id, "builder");
    assert_eq!(config.operation_timeout, Duration::from_millis(250));
}

#[test]
fn toml_parses_and_rejects_secrets_and_unknown_keys() {
    let config = KafkaConfig::from_toml(
        r#"
brokers = "127.0.0.1:9092"
client_id = "toml-client"
delivery_timeout = { secs = 5 }
connect_timeout = { secs = 1, nanos = 500000000 }
operation_timeout = 2500
"#,
    )
    .expect("合法 TOML");
    assert_eq!(config.client_id, "toml-client");
    assert_eq!(config.delivery_timeout, Duration::from_secs(5));
    assert_eq!(config.connect_timeout, Duration::from_millis(1500));
    assert_eq!(config.operation_timeout, Duration::from_millis(2500));

    let password = "brokers = \"127.0.0.1:9092\"\nsasl_password = \"top-secret\"\n";
    let error = KafkaConfig::from_toml(password).expect_err("凭据字段必须被拒绝");
    assert!(
        !error.to_string().contains("top-secret"),
        "错误不得回显密码"
    );

    let username = "brokers = \"127.0.0.1:9092\"\nsasl_username = \"admin\"\n";
    assert!(KafkaConfig::from_toml(username).is_err());

    let unknown = "brokers = \"127.0.0.1:9092\"\nsink_id = \"analytics\"\n";
    assert!(KafkaConfig::from_toml(unknown).is_err());

    assert!(
        KafkaConfig::from_toml("brokers = ").is_err(),
        "语法错误必须失败"
    );
    assert!(
        KafkaConfig::from_toml("brokers = \"broker.example.com:9092\"\n").is_err(),
        "TOML 也必须满足远程必须 TLS 的规则"
    );
}

/// 环境变量解析集中在一个测试里，避免同进程内并发读写环境变量。
#[test]
fn env_overlay_parses_and_validates() {
    // 清理，避免外部环境污染
    for key in [
        ENV_BROKERS,
        ENV_CLIENT_ID,
        ENV_SASL_MECHANISM,
        ENV_SASL_USERNAME,
        ENV_SASL_PASSWORD,
        ENV_TLS,
        ENV_TLS_CA_FILE,
        ENV_CONNECT_TIMEOUT_MS,
        ENV_OPERATION_TIMEOUT_MS,
        ENV_DELIVERY_TIMEOUT_MS,
    ] {
        std::env::remove_var(key);
    }
    KafkaConfig::from_env().expect("无环境变量时回落默认值");

    std::env::set_var(ENV_BROKERS, "127.0.0.1:9092");
    std::env::set_var(ENV_CLIENT_ID, "env-client");
    std::env::set_var(ENV_SASL_MECHANISM, "PLAIN");
    std::env::set_var(ENV_SASL_USERNAME, "env-user");
    std::env::set_var(ENV_SASL_PASSWORD, "env-password");
    std::env::set_var(ENV_TLS, "false");
    std::env::set_var(ENV_CONNECT_TIMEOUT_MS, "1500");
    std::env::set_var(ENV_OPERATION_TIMEOUT_MS, "2500");
    std::env::set_var(ENV_DELIVERY_TIMEOUT_MS, "3500");

    let config = KafkaConfig::from_env().expect("合法环境变量");
    assert_eq!(config.client_id, "env-client");
    assert_eq!(config.brokers, "127.0.0.1:9092");
    assert_eq!(config.security_protocol(), "SASL_PLAINTEXT");
    assert_eq!(config.connect_timeout, Duration::from_millis(1500));
    assert_eq!(config.operation_timeout, Duration::from_millis(2500));
    assert_eq!(config.delivery_timeout, Duration::from_millis(3500));
    assert!(
        !format!("{config:?}").contains("env-password"),
        "凭据必须脱敏"
    );

    std::env::set_var(ENV_TLS, "maybe");
    assert!(matches!(
        KafkaConfig::from_env(),
        Err(KafkaError::Config(_))
    ));

    std::env::set_var(ENV_TLS, "true");
    std::env::set_var(ENV_SASL_MECHANISM, "none");
    std::env::set_var(ENV_SASL_USERNAME, "");
    std::env::set_var(ENV_SASL_PASSWORD, "");
    let plain = KafkaConfig::from_env().expect("空凭据表示关闭 SASL");
    assert_eq!(plain.security_protocol(), "SSL");
    assert!(plain.sasl_mechanism.is_none());

    std::env::set_var(ENV_SASL_MECHANISM, "PLAIN");
    std::env::set_var(ENV_SASL_USERNAME, "env-user");
    std::env::set_var(ENV_SASL_PASSWORD, "");
    assert!(
        matches!(KafkaConfig::from_env(), Err(KafkaError::Config(_))),
        "缺少密码必须失败"
    );

    std::env::set_var(ENV_CONNECT_TIMEOUT_MS, "not-a-number");
    assert!(matches!(
        KafkaConfig::from_env(),
        Err(KafkaError::Config(_))
    ));

    for key in [
        ENV_BROKERS,
        ENV_CLIENT_ID,
        ENV_SASL_MECHANISM,
        ENV_SASL_USERNAME,
        ENV_SASL_PASSWORD,
        ENV_TLS,
        ENV_TLS_CA_FILE,
        ENV_CONNECT_TIMEOUT_MS,
        ENV_OPERATION_TIMEOUT_MS,
        ENV_DELIVERY_TIMEOUT_MS,
    ] {
        std::env::remove_var(key);
    }
}
