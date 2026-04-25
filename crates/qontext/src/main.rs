//! Orchestrator. Inits shared state ONCE, spawns the 3 components.
//! All component bodies are empty for now.

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
    qontext_core::seed::skeleton();

    let source: PathBuf = std::env::var("QONTEXT_SOURCE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./source"));
    let addr: SocketAddr = std::env::var("QONTEXT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;
    let mountpoint = std::env::var("QONTEXT_MOUNT").unwrap_or_else(|_| "/tmp/qontext".into());

    let builder = tokio::spawn(qontext_builder::run(source));
    let api = tokio::spawn(qontext_api::run(addr));
    let fs_thread = std::thread::spawn(move || {
        if let Err(e) = qontext_fs::run(&mountpoint) {
            tracing::warn!("fs daemon exited: {e:?}");
        }
    });

    tokio::select! {
        r = builder => tracing::warn!("builder ended: {r:?}"),
        r = api => tracing::warn!("api ended: {r:?}"),
    }
    let _ = fs_thread.join();
    Ok(())
}
