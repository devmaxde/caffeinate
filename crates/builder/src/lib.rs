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
pub use maxi::{render_all_md, render_entry_md, AdjEntry, EntityStore, GroupIdx, MaxiGraph};
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

/// Long-running entrypoint. Indexes once, then parks.
pub async fn run(source_dir: PathBuf) -> Result<()> {
    let res = index(&source_dir)?;
    tracing::info!(
        nodes = res.nodes.len(),
        groups = res.groups.len(),
        source = %source_dir.display(),
        "builder: indexed"
    );
    futures_park().await
}

async fn futures_park() -> Result<()> {
    std::future::pending::<()>().await;
    Ok(())
}
