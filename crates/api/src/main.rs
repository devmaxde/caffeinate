//! Standalone HTTP API. Empty stub.

use anyhow::Result;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "qontext=info".into()))
        .init();

    qontext_core::state::init();

    let addr: SocketAddr = std::env::var("QONTEXT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;

    qontext_api::run(addr).await
}
