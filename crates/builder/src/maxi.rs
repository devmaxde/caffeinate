//! Maxi-graph: per-entry context views.
//!
//! - `EntityStore`  read-optimized form of `Vec<EntityGroup>`. Owns entries.
//!   Builds per-key value-indexes only for FK keys (those mentioned in edges).
//! - `MaxiGraph`    store + adjacency by group index. 1-hop traversal only,
//!   so no recursive walks → no loop pitfalls even with cycles in the graph.
//! - `render_entry_md`  → Markdown for one entry: fields + 1-hop neighbours.
//!   Kept as its own function so the renderer is replaceable later.
//! - `render_all_md`    → parallel render of every entry, in memory.
//!
//! Conversion from the legacy `(Vec<EntityGroup>, Vec<Edge>)` shape is the
//! `MaxiGraph::from_legacy` entrypoint. Indexing + rendering use rayon.

use crate::groups::EntityGroup;
use crate::providers::Entry;
use crate::relate::Edge;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fmt::Write as _;

const MAX_NEIGHBOURS_PER_GROUP: usize = 20;
const MAX_FIELD_LEN: usize = 200;

pub struct EntityStore {
    pub groups: Vec<GroupIdx>,
    pub by_path: HashMap<String, usize>,
}

pub struct GroupIdx {
    pub name: String,
    pub source_path: String,
    pub provider: String,
    pub keys: Vec<String>,
    pub entries: Vec<Entry>,
    /// entry.id -> entries idx
    pub by_id: HashMap<String, usize>,
    /// fk key name -> field value -> entries idx list
    pub indexes: HashMap<String, HashMap<String, Vec<usize>>>,
}

#[derive(Debug, Clone)]
pub struct AdjEntry {
    pub other: usize,
    pub self_key: Option<String>,
    pub other_key: Option<String>,
    pub reason: String,
}

pub struct MaxiGraph {
    pub store: EntityStore,
    pub adjacency: Vec<Vec<AdjEntry>>,
}

impl MaxiGraph {
    /// Convert legacy structure into optimized form.
    /// Indexes only the keys that participate in adjacency edges.
    pub fn from_legacy(groups: Vec<EntityGroup>, edges: &[Edge]) -> Self {
        let by_path: HashMap<String, usize> = groups
            .iter()
            .enumerate()
            .map(|(i, g)| (g.source_path.clone(), i))
            .collect();

        // Determine which keys per group need indexing (the ones reached over an edge).
        let mut fk_keys: Vec<Vec<String>> = vec![Vec::new(); groups.len()];
        for e in edges {
            if let (Some(&ai), Some(&bi)) = (by_path.get(&e.a), by_path.get(&e.b)) {
                if let Some(k) = &e.b_key {
                    fk_keys[bi].push(k.clone());
                }
                if let Some(k) = &e.a_key {
                    fk_keys[ai].push(k.clone());
                }
            }
        }
        for v in &mut fk_keys {
            v.sort();
            v.dedup();
        }

        let mut adjacency: Vec<Vec<AdjEntry>> = vec![Vec::new(); groups.len()];
        for e in edges {
            let (Some(ai), Some(bi)) = (by_path.get(&e.a).copied(), by_path.get(&e.b).copied())
            else {
                continue;
            };
            adjacency[ai].push(AdjEntry {
                other: bi,
                self_key: e.a_key.clone(),
                other_key: e.b_key.clone(),
                reason: e.reason.clone(),
            });
            if ai != bi {
                adjacency[bi].push(AdjEntry {
                    other: ai,
                    self_key: e.b_key.clone(),
                    other_key: e.a_key.clone(),
                    reason: e.reason.clone(),
                });
            }
        }

        let group_idxs: Vec<GroupIdx> = groups
            .into_par_iter()
            .zip(fk_keys.into_par_iter())
            .map(|(g, keys)| build_group(g, &keys))
            .collect();

        MaxiGraph {
            store: EntityStore {
                groups: group_idxs,
                by_path,
            },
            adjacency,
        }
    }

    pub fn group_idx(&self, source_path: &str) -> Option<usize> {
        self.store.by_path.get(source_path).copied()
    }
}

fn build_group(g: EntityGroup, keys_to_index: &[String]) -> GroupIdx {
    let entries = g.entries;
    let mut by_id = HashMap::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        by_id.insert(e.id.clone(), i);
    }
    let mut indexes: HashMap<String, HashMap<String, Vec<usize>>> = HashMap::new();
    for k in keys_to_index {
        let mut m: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            if let Some(v) = e.meta.get(k) {
                if !v.is_empty() {
                    m.entry(v.clone()).or_default().push(i);
                }
            }
        }
        indexes.insert(k.clone(), m);
    }
    GroupIdx {
        name: g.name,
        source_path: g.source_path,
        provider: g.provider,
        keys: g.keys.into_iter().collect(),
        entries,
        by_id,
        indexes,
    }
}

/// Render one entry's context (1-hop) as Markdown.
pub fn render_entry_md(graph: &MaxiGraph, group_idx: usize, entry_idx: usize) -> String {
    let g = &graph.store.groups[group_idx];
    let entry = &g.entries[entry_idx];
    let mut out = String::new();

    let _ = writeln!(out, "# {} / {}", g.name, entry.id);
    let _ = writeln!(out);
    let _ = writeln!(out, "**source**: `{}`  ", g.source_path);
    let _ = writeln!(out, "**provider**: {}  ", g.provider);
    let _ = writeln!(out, "**entry_kind**: {}", entry.kind);
    let _ = writeln!(out);
    let _ = writeln!(out, "## Fields");
    for (k, v) in &entry.meta {
        let _ = writeln!(out, "- **{}**: {}", k, truncate(v, MAX_FIELD_LEN));
    }

    let adj = &graph.adjacency[group_idx];
    if adj.is_empty() {
        return out;
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "## Neighbours");

    for a in adj {
        let other = &graph.store.groups[a.other];
        let Some(self_key) = a.self_key.as_ref() else {
            continue;
        };
        let Some(other_key) = a.other_key.as_ref() else {
            continue;
        };
        let Some(value) = entry.meta.get(self_key).filter(|s| !s.is_empty()) else {
            continue;
        };
        let Some(idx_map) = other.indexes.get(other_key) else {
            continue;
        };
        let Some(matches) = idx_map.get(value) else {
            continue;
        };
        if matches.is_empty() {
            continue;
        }

        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "### {} ({}) — via `{}` ↔ `{}` // {}",
            other.name,
            matches.len(),
            self_key,
            other_key,
            a.reason
        );
        let cap = MAX_NEIGHBOURS_PER_GROUP.min(matches.len());
        for &mi in &matches[..cap] {
            let m = &other.entries[mi];
            let summary = entry_summary(m);
            let _ = writeln!(
                out,
                "- `{}/{}` — {}",
                other.name,
                m.id,
                truncate(&summary, MAX_FIELD_LEN)
            );
        }
        if matches.len() > cap {
            let _ = writeln!(out, "- … +{} more", matches.len() - cap);
        }
    }
    out
}

fn entry_summary(e: &Entry) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (k, v) in e.meta.iter().take(3) {
        parts.push(format!("{}={}", k, truncate(v, 40)));
    }
    parts.join(", ")
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(n).collect();
        t.push('…');
        t
    }
}

/// Render every entry in parallel. Key = "<source_path>#<id>".
pub fn render_all_md(graph: &MaxiGraph) -> HashMap<String, String> {
    let pairs_idx: Vec<(usize, usize)> = graph
        .store
        .groups
        .iter()
        .enumerate()
        .flat_map(|(gi, g)| (0..g.entries.len()).map(move |ei| (gi, ei)))
        .collect();

    pairs_idx
        .par_iter()
        .map(|&(gi, ei)| {
            let g = &graph.store.groups[gi];
            // idx in key guarantees uniqueness when inferred id is non-unique.
            let key = format!("{}#{:08}_{}", g.source_path, ei, g.entries[ei].id);
            let md = render_entry_md(graph, gi, ei);
            (key, md)
        })
        .collect()
}
