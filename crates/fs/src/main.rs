//! Standalone FUSE3 daemon. Empty stub.

use anyhow::Result;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "qontext=info".into()))
        .init();

    qontext_core::state::init();
    qontext_core::seed::skeleton();

    let mountpoint = std::env::var("QONTEXT_MOUNT").unwrap_or_else(|_| "/tmp/qontext".into());
    qontext_fs::run(&mountpoint)
}
