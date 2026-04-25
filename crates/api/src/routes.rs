use axum::{Router, routing::get};
use tower_http::cors::CorsLayer;

pub fn build() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .layer(CorsLayer::permissive())
}

async fn healthz() -> &'static str {
    "ok"
}
