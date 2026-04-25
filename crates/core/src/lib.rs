//! Shared engine primitives: state (evmap), model, seed data, IDs.
//!
//! Reader side: `state::reader()` -> ReadHandle clone (per thread).
//! Writer side: `state::with_writer(|wh| ...)` -> exclusive WriteHandle.
//! Convenience: `state::read_node(path)` and `state::upsert_node(path, node)`.

pub mod ids;
pub mod model;
pub mod seed;
pub mod state;

pub use model::{FileNode, NodeKind};
pub use state::{Node, NodePath};
