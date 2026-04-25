//! Empty placeholder. Real seeding is the builder's job.
//!
//! Optional `skeleton()` writes the canonical entity-kind directories so a
//! standalone fs/api binary mounts a non-empty tree during dev.

use crate::model::FileNode;
use crate::state;

pub fn skeleton() {
    let dirs: &[(&str, &[&str])] = &[
        ("/", &["/customers", "/employees", "/projects", "/_graph"]),
        ("/customers", &[]),
        ("/employees", &[]),
        ("/projects", &[]),
        ("/_graph", &[]),
    ];
    let entries = dirs.iter().map(|(p, kids)| {
        let node = FileNode::new_dir(kids.iter().map(|s| s.to_string()).collect());
        (p.to_string(), node)
    });
    state::upsert_batch(entries);
}
