//! Cross-reference inference + entity adjacency graph.
//!
//! Two paths to edges:
//! - `infer_edges_llm`  — sends group summaries to Claude via `rig`,
//!   parses JSON edge list. Requires `ANTHROPIC_API_KEY`.
//! - `heuristic_edges`  — links groups that share an id-ish key. No network.
//!
//! `EntityGraph::build(groups, edges)` materializes an adjacency map keyed
//! by `source_path`. Each node lives behind `Arc<Mutex<...>>`, neighbors
//! are `Weak` to keep cycles safe (you only ever traverse one hop).

use crate::groups::EntityGroup;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    /// `source_path` of one side.
    pub a: String,
    /// `source_path` of the other side.
    pub b: String,
    pub a_key: Option<String>,
    pub b_key: Option<String>,
    pub reason: String,
}

#[derive(Debug)]
pub struct EntityNode {
    pub name: String,
    pub source_path: String,
    pub provider: String,
    pub keys: Vec<String>,
    pub entry_count: usize,
    pub neighbors: Vec<EdgeRef>,
}

#[derive(Debug)]
pub struct EdgeRef {
    pub target: Weak<Mutex<EntityNode>>,
    pub self_key: Option<String>,
    pub other_key: Option<String>,
    pub reason: String,
}

pub struct EntityGraph {
    pub nodes: BTreeMap<String, Arc<Mutex<EntityNode>>>,
}

impl EntityGraph {
    pub fn build(groups: &[EntityGroup], edges: &[Edge]) -> Self {
        let mut nodes: BTreeMap<String, Arc<Mutex<EntityNode>>> = BTreeMap::new();
        for g in groups {
            let n = EntityNode {
                name: g.name.clone(),
                source_path: g.source_path.clone(),
                provider: g.provider.clone(),
                keys: g.keys.iter().cloned().collect(),
                entry_count: g.entries.len(),
                neighbors: Vec::new(),
            };
            nodes.insert(g.source_path.clone(), Arc::new(Mutex::new(n)));
        }

        for e in edges {
            let (Some(a), Some(b)) = (nodes.get(&e.a).cloned(), nodes.get(&e.b).cloned()) else {
                continue;
            };
            if Arc::ptr_eq(&a, &b) {
                // self-loop: still record one direction
                a.lock().unwrap().neighbors.push(EdgeRef {
                    target: Arc::downgrade(&b),
                    self_key: e.a_key.clone(),
                    other_key: e.b_key.clone(),
                    reason: e.reason.clone(),
                });
                continue;
            }
            a.lock().unwrap().neighbors.push(EdgeRef {
                target: Arc::downgrade(&b),
                self_key: e.a_key.clone(),
                other_key: e.b_key.clone(),
                reason: e.reason.clone(),
            });
            b.lock().unwrap().neighbors.push(EdgeRef {
                target: Arc::downgrade(&a),
                self_key: e.b_key.clone(),
                other_key: e.a_key.clone(),
                reason: e.reason.clone(),
            });
        }
        EntityGraph { nodes }
    }

    /// (neighbor name, reason) pairs for one node.
    pub fn neighbors_of(&self, source_path: &str) -> Vec<(String, String, String)> {
        let Some(n) = self.nodes.get(source_path) else {
            return vec![];
        };
        let n = n.lock().unwrap();
        n.neighbors
            .iter()
            .filter_map(|e| {
                let t = e.target.upgrade()?;
                let t = t.lock().unwrap();
                Some((t.name.clone(), t.source_path.clone(), e.reason.clone()))
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Build the JSON payload sent to the LLM. Path is included intentionally —
/// directory names like "Customer_Relation_Management" carry domain context.
pub fn build_summaries(groups: &[EntityGroup]) -> serde_json::Value {
    let arr: Vec<_> = groups
        .iter()
        .map(|g| {
            let samples: Vec<_> = g.entries.iter().take(3).map(|e| &e.meta).collect();
            json!({
                "id":            g.source_path,
                "name":          g.name,
                "path":          g.source_path,
                "provider":      g.provider,
                "total_entries": g.entries.len(),
                "keys":          g.keys.iter().collect::<Vec<_>>(),
                "samples":       samples,
            })
        })
        .collect();
    json!({ "groups": arr })
}

/// Heuristic: link groups that share an id-ish key (case-insensitive).
/// Used as a fallback when the LLM is not available, and as a sanity baseline.
pub fn heuristic_edges(groups: &[EntityGroup]) -> Vec<Edge> {
    let mut out = Vec::new();
    for (i, a) in groups.iter().enumerate() {
        for b in &groups[i + 1..] {
            for k in &a.keys {
                let kl = k.to_ascii_lowercase();
                let id_like = kl == "id"
                    || kl.ends_with("_id")
                    || kl.contains("name")
                    || kl.ends_with("_paths");
                if !id_like {
                    continue;
                }
                let m = b.keys.iter().find(|bk| bk.eq_ignore_ascii_case(k));
                if let Some(bk) = m {
                    out.push(Edge {
                        a: a.source_path.clone(),
                        b: b.source_path.clone(),
                        a_key: Some(k.clone()),
                        b_key: Some(bk.clone()),
                        reason: format!("shared key `{}`", k),
                    });
                    break;
                }
            }
        }
    }
    out
}

const PREAMBLE: &str = r#"You are a schema relationship analyst.

Input: a list of entity groups. Each group has:
  - id   = unique source path (use this in edges)
  - name = friendly label
  - path = filesystem path; PARENT DIRECTORIES carry domain context
           (e.g. "Customer_Relation_Management/customers.json" → customers)
  - keys = schema field names
  - samples = up to 3 example records

Task: identify cross-references between groups.
- Edges are NON-DIRECTIONAL. Self-loops and cycles are allowed.
- Use key-name overlap (e.g. customer_id ↔ customer_id) as a strong signal.
- Use semantic hints from path / sample content for unstructured data
  (e.g. an emails group may reference an employees group via author names).
- Set a_key / b_key to null when the link is implicit, not via a column.
- Keep `reason` under 120 chars.

Respond with ONLY a JSON object, no markdown fences:
{
  "edges": [
    {"a": "<group id>", "b": "<group id>", "a_key": "...", "b_key": "...", "reason": "..."}
  ]
}"#;

#[derive(Deserialize)]
struct EdgeList {
    edges: Vec<Edge>,
}

pub const DEFAULT_MODEL: &str = "anthropic/claude-haiku-4.5";

pub async fn infer_edges_llm(groups: &[EntityGroup]) -> Result<Vec<Edge>> {
    let (response, _model) = call_llm_raw(groups).await?;
    let trimmed = strip_fences(&response);
    let parsed: EdgeList = serde_json::from_str(trimmed)
        .with_context(|| format!("parse llm response: {}", response))?;
    Ok(parsed.edges)
}

/// Call the LLM and return (raw_response, model_used). Useful for tests that
/// want to dump the raw JSON for inspection alongside the parsed graph.
pub async fn call_llm_raw(groups: &[EntityGroup]) -> Result<(String, String)> {
    use rig::client::{CompletionClient, ProviderClient};
    use rig::completion::Prompt;
    use rig::providers::openrouter;

    let _ = dotenvy::dotenv();
    std::env::var("OPENROUTER_API_KEY").context("OPENROUTER_API_KEY not set")?;
    let model = std::env::var("QONTEXT_LLM_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());

    let payload = serde_json::to_string_pretty(&build_summaries(groups))?;
    let client = openrouter::Client::from_env();
    let agent = client.agent(&model).preamble(PREAMBLE).build();

    let response: String = agent
        .prompt(payload.as_str())
        .await
        .with_context(|| format!("openrouter prompt failed (model={})", model))?;
    Ok((response, model))
}

fn strip_fences(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("```json")
        .or_else(|| s.strip_prefix("```"))
        .unwrap_or(s);
    let s = s.strip_suffix("```").unwrap_or(s);
    s.trim()
}
