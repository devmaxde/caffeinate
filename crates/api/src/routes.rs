//! HTTP surface over `qontext_core::state` evmap.
//!
//! - `GET /healthz`              liveness
//! - `GET /`                     read root dir node
//! - `GET /{*path}`              read by logical path; files return raw content,
//!                                dirs return JSON listing
//! - `GET /_meta/{*path}`        full `FileNode` JSON (debug / inspection)
//! - `POST /` and `POST /{*path}` upsert a node from JSON body

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use qontext_core::model::{FileNode, NodeKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tower_http::cors::CorsLayer;

pub fn build() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/_meta", get(get_meta_root))
        .route("/_meta/", get(get_meta_root))
        .route("/_meta/*path", get(get_meta))
        .route("/", get(get_root).post(post_root))
        .route("/*path", get(get_node).post(post_node))
        .layer(CorsLayer::permissive())
}

async fn healthz() -> &'static str {
    "ok"
}

fn to_logical(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".into()
    } else {
        format!("/{}", trimmed)
    }
}

#[derive(Serialize)]
struct DirView<'a> {
    path: &'a str,
    kind: &'static str,
    size: u64,
    mtime_secs: u64,
    etag: u64,
    children: &'a [String],
    meta: &'a BTreeMap<String, String>,
}

fn render_node(path: &str, node: &FileNode) -> Response {
    match node.kind {
        NodeKind::Dir => Json(DirView {
            path,
            kind: "dir",
            size: node.size,
            mtime_secs: node.mtime_secs,
            etag: node.etag,
            children: &node.children,
            meta: &node.meta,
        })
        .into_response(),
        NodeKind::Link => {
            let target = node.meta.get("link_target").cloned().unwrap_or_default();
            Json(serde_json::json!({
                "path": path,
                "kind": "link",
                "target": target,
                "meta": node.meta,
                "etag": node.etag,
            }))
            .into_response()
        }
        NodeKind::File => {
            let ct = guess_content_type(path);
            (StatusCode::OK, [(header::CONTENT_TYPE, ct)], node.content.clone()).into_response()
        }
    }
}

fn guess_content_type(path: &str) -> &'static str {
    if path.ends_with(".md") {
        "text/markdown; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".csv") {
        "text/csv; charset=utf-8"
    } else if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else {
        "text/plain; charset=utf-8"
    }
}

async fn get_root() -> Response {
    read_response("/")
}

async fn get_node(Path(path): Path<String>) -> Response {
    let lp = to_logical(&path);
    read_response(&lp)
}

fn read_response(lp: &str) -> Response {
    match qontext_core::state::read_node(lp) {
        Some(n) => render_node(lp, &n),
        None => (StatusCode::NOT_FOUND, format!("no node at {}", lp)).into_response(),
    }
}

async fn get_meta_root() -> Response {
    meta_response("/")
}

async fn get_meta(Path(path): Path<String>) -> Response {
    let lp = to_logical(&path);
    meta_response(&lp)
}

fn meta_response(lp: &str) -> Response {
    match qontext_core::state::read_node(lp) {
        Some(n) => Json(&*n).into_response(),
        None => (StatusCode::NOT_FOUND, format!("no node at {}", lp)).into_response(),
    }
}

#[derive(Deserialize)]
struct UpsertReq {
    /// "file" (default), "dir", or "link"
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    children: Option<Vec<String>>,
    #[serde(default)]
    meta: Option<BTreeMap<String, String>>,
}

#[derive(Serialize)]
struct UpsertResp {
    path: String,
    kind: &'static str,
    etag: u64,
    size: u64,
}

async fn post_root(Json(req): Json<UpsertReq>) -> Response {
    upsert("/", req)
}

async fn post_node(Path(path): Path<String>, Json(req): Json<UpsertReq>) -> Response {
    let lp = to_logical(&path);
    upsert(&lp, req)
}

fn upsert(path: &str, req: UpsertReq) -> Response {
    let kind_str = req.kind.as_deref().unwrap_or("file");
    let prev = qontext_core::state::read_node(path);

    let mut node = match kind_str {
        "dir" => FileNode::new_dir(req.children.unwrap_or_default()),
        "link" => {
            let target = req
                .meta
                .as_ref()
                .and_then(|m| m.get("link_target").cloned())
                .unwrap_or_default();
            FileNode::new_link(target)
        }
        "file" => FileNode::new_file(req.content.unwrap_or_default()),
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown kind `{}` (expected file|dir|link)", other),
            )
                .into_response();
        }
    };

    if let Some(meta) = req.meta {
        for (k, v) in meta {
            node = node.with_meta(k, v);
        }
    }
    if let Some(prev) = prev {
        node.etag = prev.etag;
    }
    let node = node.touched();
    let resp = UpsertResp {
        path: path.to_string(),
        kind: match node.kind {
            NodeKind::File => "file",
            NodeKind::Dir => "dir",
            NodeKind::Link => "link",
        },
        etag: node.etag,
        size: node.size,
    };
    qontext_core::state::upsert_node(path.to_string(), node);
    (StatusCode::OK, Json(resp)).into_response()
}
