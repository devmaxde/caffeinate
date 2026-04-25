//! Shared state: evmap with one writer + many readers, behind a `OnceLock`.
//!
//! Reader pattern (FUSE / HTTP / WS):
//! ```ignore
//! let r = qontext_core::state::reader();
//! if let Some(node) = qontext_core::state::read_node("/employees/jane.md") {
//!     // use node...
//! }
//! ```
//!
//! Writer pattern (builder):
//! ```ignore
//! qontext_core::state::upsert_node("/employees/jane.md".into(), file_node);
//! // refresh is automatic in the convenience helper.
//! ```

use crate::model::FileNode;
use evmap::{ReadHandle, ReadHandleFactory, WriteHandle};
use std::sync::{Arc, Mutex, OnceLock};

pub type NodePath = String;
pub type Node = Arc<FileNode>;

static FACTORY: OnceLock<ReadHandleFactory<NodePath, Node>> = OnceLock::new();
static WRITER: OnceLock<Mutex<WriteHandle<NodePath, Node>>> = OnceLock::new();

/// Initialize the shared state. Call exactly once at process startup,
/// before any thread tries to read or write.
pub fn init() {
    let (read, write) = evmap::new::<NodePath, Node>();
    FACTORY
        .set(read.factory())
        .map_err(|_| ())
        .expect("state::init called twice");
    WRITER
        .set(Mutex::new(write))
        .map_err(|_| ())
        .expect("state::init called twice");
}

/// True after `init()` has been called.
pub fn is_initialized() -> bool {
    FACTORY.get().is_some() && WRITER.get().is_some()
}

/// Get a fresh `ReadHandle` for the current thread. Each thread should call this
/// once and clone the result if it spawns more readers.
pub fn reader() -> ReadHandle<NodePath, Node> {
    FACTORY
        .get()
        .expect("qontext_core::state::init() not called")
        .handle()
}

/// Run a closure with exclusive access to the writer. Lock is uncontended in
/// production (single builder task), but the Mutex guards us during dev when
/// multiple solo binaries might race.
pub fn with_writer<F, R>(f: F) -> R
where
    F: FnOnce(&mut WriteHandle<NodePath, Node>) -> R,
{
    let mtx = WRITER
        .get()
        .expect("qontext_core::state::init() not called");
    let mut guard = mtx.lock().expect("writer mutex poisoned");
    f(&mut guard)
}

/// Read a single node by full path. Returns None if missing.
pub fn read_node(path: &str) -> Option<Node> {
    let r = reader();
    r.get(path).and_then(|guard| guard.iter().next().cloned())
}

/// Replace (or insert) a node at `path` and publish to readers.
/// Single-value semantics: any prior values for this key are dropped.
pub fn upsert_node(path: NodePath, node: FileNode) {
    let arc = Arc::new(node);
    with_writer(|wh| {
        wh.empty(path.clone());
        wh.insert(path, arc);
        wh.refresh();
    });
}

/// Batch-upsert without an intermediate refresh per item.
pub fn upsert_batch(items: impl IntoIterator<Item = (NodePath, FileNode)>) {
    with_writer(|wh| {
        for (path, node) in items {
            let arc = Arc::new(node);
            wh.empty(path.clone());
            wh.insert(path, arc);
        }
        wh.refresh();
    });
}

/// List all paths currently in the map. O(n) — used by demo / debug only.
pub fn list_paths() -> Vec<NodePath> {
    let r = reader();
    r.read()
        .map(|guard| guard.iter().map(|(k, _)| k.clone()).collect())
        .unwrap_or_default()
}

/// Total node count.
pub fn len() -> usize {
    let r = reader();
    r.len()
}
