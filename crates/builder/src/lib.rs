//! Builder = single-writer side of the engine. Today: cold-start scan,
//! per-file entity-group extraction, optional LLM relationship inference.
//! No watcher yet.

use anyhow::Result;
use std::path::{Path, PathBuf};

pub mod groups;
pub mod maxi;
pub mod providers;
pub mod relate;
pub mod render;
pub mod source;

pub use groups::EntityGroup;
pub use maxi::{
    render_all_as_nodes, render_all_md, render_entry_md, AdjEntry, EntityStore, GroupIdx,
    MaxiGraph, MAXI_ROOT,
};
pub use relate::{Edge, EdgeRef, EntityGraph, EntityNode};
pub use render::{scan, IndexResult, Pair};

/// Hardcoded source until we wire CLI/env. Resolves to repo's EnterpriseBench/.
pub fn default_source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../EnterpriseBench")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("EnterpriseBench"))
}

/// Scan + upsert file mirror nodes into shared evmap. Returns counts + groups.
/// Caller must have run `qontext_core::state::init()` first.
pub fn index(source_dir: &Path) -> Result<IndexResult> {
    let result = scan(source_dir)?;
    qontext_core::state::upsert_batch(result.nodes.clone());
    Ok(result)
}

/// Long-running entrypoint. Indexes once, builds the maxi graph, upserts
/// every per-entry markdown view as a `FileNode` under `/maxi/...`, then parks.
pub async fn run(source_dir: PathBuf) -> Result<()> {
    let res = index(&source_dir)?;
    tracing::info!(
        nodes = res.nodes.len(),
        groups = res.groups.len(),
        source = %source_dir.display(),
        "builder: indexed file mirror"
    );

    let edges = if std::env::var("OPENROUTER_API_KEY").is_ok() {
        match relate::infer_edges_llm(&res.groups).await {
            Ok(es) => {
                tracing::info!(count = es.len(), "builder: edges from llm");
                es
            }
            Err(e) => {
                tracing::warn!(error = %e, "builder: llm edges failed, using heuristic");
                relate::heuristic_edges(&res.groups)
            }
        }
    } else {
        let es = relate::heuristic_edges(&res.groups);
        tracing::info!(count = es.len(), "builder: edges from heuristic");
        es
    };

    let graph = MaxiGraph::from_legacy(res.groups, &edges);
    let maxi_nodes = render_all_as_nodes(&graph);
    let n = maxi_nodes.len();
    qontext_core::state::upsert_batch(maxi_nodes);
    tracing::info!(maxi_nodes = n, "builder: maxi rendered into evmap");

    futures_park().await
}

async fn futures_park() -> Result<()> {
    std::future::pending::<()>().await;
    Ok(())
}
