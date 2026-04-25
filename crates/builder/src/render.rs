//! Walk a source root + run providers → produce both:
//! - the file-mirror node graph (FileNodes for evmap)
//! - the per-file entity groups (Vec<type>)

use crate::groups::EntityGroup;
use crate::providers::Registry;
use crate::source;
use anyhow::{Context, Result};
use qontext_core::model::FileNode;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub type Pair = (String, FileNode);

pub struct IndexResult {
    pub nodes: Vec<Pair>,
    pub groups: Vec<EntityGroup>,
}

pub fn scan(root: &Path) -> Result<IndexResult> {
    let registry = Registry::with_default();
    let files = source::walk(root)?;

    let mut children_of: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut nodes: BTreeMap<String, FileNode> = BTreeMap::new();
    let mut groups: Vec<EntityGroup> = Vec::new();
    children_of.entry("/".into()).or_default();

    for file in &files {
        let rel = file
            .strip_prefix(root)
            .with_context(|| format!("strip prefix {}", file.display()))?;
        let lp = to_logical(rel);
        register_ancestors(&mut children_of, &lp);

        let raw = std::fs::read(file)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let mut fnode = FileNode::new_file(raw);

        if let Some(prov) = registry.for_path(file) {
            match prov.fetch(file) {
                Ok(entries) => {
                    let edir = format!("{}__entries", lp);
                    register_ancestors(&mut children_of, &edir);
                    let kids = children_of.entry(edir.clone()).or_default();

                    for e in &entries {
                        let epath = format!("{}/{}", edir, e.id);
                        let mut en = FileNode::new_file(e.text.clone())
                            .with_meta("provider", prov.name())
                            .with_meta("entry_kind", e.kind.clone())
                            .with_meta("source_file", lp.clone());
                        for (k, v) in &e.meta {
                            en = en.with_meta(k.clone(), v.clone());
                        }
                        kids.insert(epath.clone());
                        nodes.insert(epath, en);
                    }
                    fnode = fnode
                        .with_meta("provider", prov.name())
                        .with_meta("entries_dir", edir);

                    let stem = file
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("group")
                        .to_string();
                    groups.push(EntityGroup::new(
                        stem,
                        lp.clone(),
                        prov.name().to_string(),
                        entries,
                    ));
                }
                Err(e) => {
                    fnode = fnode.with_meta("provider_error", e.to_string());
                }
            }
        }
        nodes.insert(lp, fnode);
    }

    for (dir, kids) in children_of {
        let kids_vec: Vec<String> = kids.into_iter().collect();
        nodes.insert(dir, FileNode::new_dir(kids_vec));
    }

    Ok(IndexResult {
        nodes: nodes.into_iter().collect(),
        groups,
    })
}

fn register_ancestors(children_of: &mut BTreeMap<String, BTreeSet<String>>, path: &str) {
    let parent = parent_of(path);
    children_of
        .entry(parent.clone())
        .or_default()
        .insert(path.to_string());
    if parent != "/" {
        register_ancestors(children_of, &parent);
    }
}

fn parent_of(p: &str) -> String {
    match p.rfind('/') {
        Some(0) => "/".into(),
        Some(i) => p[..i].into(),
        None => "/".into(),
    }
}

fn to_logical(rel: &Path) -> String {
    let mut s = String::from("/");
    let mut first = true;
    for comp in rel.components() {
        if !first {
            s.push('/');
        }
        first = false;
        s.push_str(&comp.as_os_str().to_string_lossy());
    }
    s
}
