use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeKind {
    File,
    Dir,
    Link,
}

/// Minimal node. Lives behind `Arc` in the evmap. Format-agnostic on purpose:
/// builder writes whatever bytes it wants in `content`, encodes domain hints
/// in `meta`. Core has no idea what an "entity", "edge", or "provenance" is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileNode {
    pub kind: NodeKind,
    pub content: String,
    /// Child paths for dirs (full paths, not just names). Empty for files/links.
    pub children: Vec<String>,
    pub size: u64,
    pub mtime_secs: u64,
    /// Monotonic version, bumped per writer update. WS uses it to detect changes.
    pub etag: u64,
    /// Builder-defined tags. Examples: entity_type, entity_id, link_target,
    /// source_uri, confidence_x100, resolved_by, human_notes.
    pub meta: BTreeMap<String, String>,
}

impl FileNode {
    pub fn new_dir(children: Vec<String>) -> Self {
        Self {
            kind: NodeKind::Dir,
            content: String::new(),
            children,
            size: 0,
            mtime_secs: now_secs(),
            etag: 0,
            meta: BTreeMap::new(),
        }
    }

    pub fn new_file(content: impl Into<String>) -> Self {
        let content = content.into();
        let size = content.len() as u64;
        Self {
            kind: NodeKind::File,
            content,
            children: vec![],
            size,
            mtime_secs: now_secs(),
            etag: 0,
            meta: BTreeMap::new(),
        }
    }

    /// Pointer to another node. `target` is stored in `meta["link_target"]`.
    /// Renderers (FUSE / API) follow at read time if they want.
    pub fn new_link(target: impl Into<String>) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert("link_target".into(), target.into());
        Self {
            kind: NodeKind::Link,
            content: String::new(),
            children: vec![],
            size: 0,
            mtime_secs: now_secs(),
            etag: 0,
            meta,
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Dir)
    }

    pub fn touched(mut self) -> Self {
        self.mtime_secs = now_secs();
        self.etag = self.etag.wrapping_add(1);
        self.size = self.content.len() as u64;
        self
    }

    pub fn with_meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.meta.insert(key.into(), value.into());
        self
    }
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
