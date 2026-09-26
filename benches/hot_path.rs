#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! kafkax 热路径基准测试：配置加载 + 校验（无网络）。
use std::hint::black_box;
use std::time::Instant;

use kafkax::{
    partition_for_key, KafkaConfig, ENV_BROKERS, ENV_CLIENT_ID, ENV_CONNECT_TIMEOUT_MS,
    ENV_DELIVERY_TIMEOUT_MS, ENV_OPERATION_TIMEOUT_MS, ENV_SASL_MECHANISM, ENV_SASL_PASSWORD,
    ENV_SASL_USERNAME, ENV_TLS, ENV_TLS_CA_FILE,
};

fn isolate_env() {
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

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn load_once() -> KafkaConfig {
    let config = KafkaConfig::from_env().expect("配置加载失败");
    config.validate().expect("配置校验失败");
    config
}

fn main() {
    isolate_env();
    let n = iters();
    // 预热
    for _ in 0..n.min(50) {
        let config = load_once();
        black_box(&config);
        black_box(partition_for_key(b"warmup-key", 8));
    }
    let start = Instant::now();
    for i in 0..n {
        let config = load_once();
        black_box(&config);
        // 附带覆盖消息路由纯函数热路径
        let key = format!("key-{i}");
        black_box(partition_for_key(key.as_bytes(), 8));
    }
    let elapsed = start.elapsed();
    println!(
        "bench_kafkax_hot_path: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}
