//! Standalone HTTP API. Spawns the builder so the evmap is populated, then
//! serves `qontext_core::state` over HTTP.

use anyhow::Result;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "qontext=info".into()))
        .init();

    qontext_core::state::init();

    let source = std::env::var("QONTEXT_SOURCE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| qontext_builder::default_source());
    tracing::info!(source = %source.display(), "qontext-api: starting builder");
    tokio::spawn(async move {
        if let Err(e) = qontext_builder::run(source).await {
            tracing::error!(error = %e, "builder run failed");
        }
    });

    let addr: SocketAddr = std::env::var("QONTEXT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;

    qontext_api::run(addr).await
}
