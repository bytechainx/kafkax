//! [`KafkaPool`] 的构造面：建连、校验式构造与 rustls 客户端配置。
//!
//! 从门面 `connection.rs` 下沉（`MR-STRUCT-007` 腾余量）。`connect_inner` 与
//! `build_tls_config` 只在本模块内互调，**全程无可见性调整**。

use super::*;

impl KafkaPool {
    /// 按配置建立连接（含 TLS / SASL 协商），受 `connect_timeout` 约束。
    ///
    /// # Errors
    ///
    /// 配置非法、超时或 broker 不可达。
    pub async fn connect(config: KafkaConfig) -> KafkaResult<Self> {
        config.validate()?;
        let connect_timeout = config.connect_timeout;
        tokio::time::timeout(connect_timeout, Self::connect_inner(config))
            .await
            .map_err(|_| KafkaError::Timeout("kafkax connect 超时".into()))?
    }

    /// 从 `FOUNDATIONX_KAFKAX_*` 环境变量加载配置并连接。
    ///
    /// # Errors
    ///
    /// 与 [`KafkaPool::connect`] 相同。
    pub async fn connect_from_env() -> KafkaResult<Self> {
        Self::connect(KafkaConfig::from_env()?).await
    }

    /// 同步构造：仅校验配置，**不建立网络连接**。
    ///
    /// 该池可用于读取配置/统计与生命周期管理；任何需要 broker 的操作
    /// （[`Self::ping`]、[`Self::health_check`]、[`Self::producer`] 的发布等）都会返回
    /// [`KafkaError::Connection`]。需要真实 I/O 时请用 [`Self::connect`]。
    ///
    /// # Errors
    ///
    /// 配置非法时返回 [`KafkaError::Config`]。
    pub fn new(config: KafkaConfig) -> KafkaResult<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                client: None,
                published: AtomicU64::new(0),
                publish_failed: AtomicU64::new(0),
                publish_timeouts: AtomicU64::new(0),
                publish_cancelled: AtomicU64::new(0),
                topics_ensured: AtomicU64::new(0),
                topics_deleted: AtomicU64::new(0),
                lifecycle: Lifecycle::new(),
            }),
        })
    }

    async fn connect_inner(config: KafkaConfig) -> KafkaResult<Self> {
        let brokers: Vec<String> = config
            .brokers
            .split(',')
            .map(|broker| broker.trim().to_string())
            .filter(|broker| !broker.is_empty())
            .collect();
        let mut builder = ClientBuilder::new(brokers).client_id(config.client_id.clone());
        if config.tls {
            builder = builder.tls_config(build_tls_config(config.tls_ca_file.clone()).await?);
        }
        if let Some((username, password)) = config.sasl_credentials() {
            builder = builder.sasl_config(SaslConfig::Plain(Credentials::new(
                username.to_owned(),
                password.to_owned(),
            )));
        } else if config.sasl_mechanism.is_some() {
            return Err(KafkaError::Config(
                "SASL 机制已设置但缺少 username/password".into(),
            ));
        }
        let client = builder
            .build()
            .await
            .map_err(|error| map_kafka_error("kafkax connect", error))?;
        tracing::debug!(
            protocol = config.security_protocol(),
            client_id = %config.client_id,
            "kafkax 连接已建立"
        );
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                client: Some(client),
                published: AtomicU64::new(0),
                publish_failed: AtomicU64::new(0),
                publish_timeouts: AtomicU64::new(0),
                publish_cancelled: AtomicU64::new(0),
                topics_ensured: AtomicU64::new(0),
                topics_deleted: AtomicU64::new(0),
                lifecycle: Lifecycle::new(),
            }),
        })
    }
}

/// 构建 rustls 客户端配置（公共根证书 + 可选自定义 PEM CA）。
///
/// 内部会尝试安装 ring crypto provider；若已有 provider 则跳过（支持多次 `connect()` 调用），
/// 若安装因其他原因失败则 fail-fast 返回 [`KafkaError::Config`] 而非静默吞错。
async fn build_tls_config(ca_file: Option<PathBuf>) -> KafkaResult<Arc<rustls::ClientConfig>> {
    tokio::task::spawn_blocking(move || {
        // 仅在尚未安装 crypto provider 时尝试安装；安装失败则 fail-fast
        // 而非留到后续 TLS 操作时运行时 panic。
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            rustls::crypto::ring::default_provider()
                .install_default()
                // CryptoProvider 未实现 Display，用 Debug 输出错误详情
                .map_err(|e| KafkaError::Config(format!("TLS crypto provider 安装失败: {e:?}")))?;
        }
        let mut roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = ca_file {
            let metadata = std::fs::metadata(&path)
                .map_err(|error| KafkaError::Config(format!("无法检查 TLS CA 文件: {error}")))?;
            if !metadata.is_file() {
                return Err(KafkaError::Config("TLS CA 路径必须是普通文件".into()));
            }
            if metadata.len() > 1024 * 1024 {
                return Err(KafkaError::Config("TLS CA 文件不得超过 1 MiB".into()));
            }
            let file = std::fs::File::open(&path)
                .map_err(|error| KafkaError::Config(format!("无法读取 TLS CA 文件: {error}")))?;
            let mut reader = std::io::BufReader::new(file);
            let certificates = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| KafkaError::Config(format!("TLS CA PEM 解析失败: {error}")))?;
            if certificates.is_empty() {
                return Err(KafkaError::Config("TLS CA 文件中没有证书".into()));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|error| KafkaError::Config(format!("TLS CA 证书无效: {error}")))?;
            }
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(tls))
    })
    .await
    .map_err(|error| KafkaError::Connection(format!("TLS 配置任务失败: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证无 CA 文件时 `build_tls_config` 可正常构造 `ClientConfig`。
    #[tokio::test]
    async fn build_tls_config_without_ca_succeeds() {
        let config = build_tls_config(None)
            .await
            .expect("无 CA 文件的 TLS 配置应构造成功");
        // rustls ClientConfig 内部字段不公开；验证 Arc 引用计数与类型正确性
        assert!(Arc::strong_count(&config) >= 1);
    }

    /// 验证 crypto provider 已安装时重复调用不报错（`install_default` 的 benign 路径）。
    #[tokio::test]
    async fn build_tls_config_twice_is_idempotent() {
        let first = build_tls_config(None).await.expect("首次 TLS 配置应成功");
        let second = build_tls_config(None)
            .await
            .expect("重复 TLS 配置应成功（已有 provider 时 benign 跳过）");
        // 两次调用产生不同 Arc 实例但功能等价
        assert!(Arc::strong_count(&first) >= 1);
        assert!(Arc::strong_count(&second) >= 1);
    }
}
