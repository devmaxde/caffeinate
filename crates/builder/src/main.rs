//! Standalone builder. Empty stub.

use anyhow::Result;
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
        .unwrap_or_else(|_| PathBuf::from("./source"));

    qontext_builder::run(source).await
}
