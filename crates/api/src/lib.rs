//! HTTP API over `qontext_core::state`. Empty for now — only `/healthz`.
//! See `RUST_PLAN.md §6` for the planned routes.

pub mod routes;

use anyhow::Result;
use axum::Router;
use std::net::SocketAddr;

pub async fn run(addr: SocketAddr) -> Result<()> {
    let app = router();
    tracing::info!(%addr, "qontext-api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn router() -> Router {
    routes::build()
}
