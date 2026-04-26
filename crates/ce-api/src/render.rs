//! On-demand LLM rendering of an entity to markdown, with tool calls so the
//! model can pull graph context it deems necessary. Uses Gemini's
//! function-calling API directly (REST, no SDK).

use std::collections::{BTreeSet, HashMap};
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};

use ce_llm::LlmConfig;
use ce_store::Store;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

use crate::ApiError;

/// Render `(entity_type, id)` as Markdown by calling the configured LLM with
/// a small toolbox over the fact graph. Returns the model's final text.
///
/// If `md_store_root` is `Some`, the function first checks
/// `<root>/<etype>/<id>.md`; on hit it returns the cached body without
/// touching the LLM. On miss it generates and writes the file before
/// returning. Pass `None` to force-regenerate (and skip the disk write).
///
/// Takes a `store_path` rather than a `&Store` because the rusqlite
/// `Connection` inside Store is `!Sync`; holding it across an `.await` makes
/// the handler future `!Send`. Each tool call opens its own short-lived
/// connection (sqlite WAL is fine with concurrent readers).
pub async fn render_entity_markdown(
    store_path: &FsPath,
    cfg: &LlmConfig,
    entity_type: &str,
    id: &str,
    md_store_root: Option<&FsPath>,
) -> Result<String, ApiError> {
    if let Some(root) = md_store_root {
        match crate::md_cache::read_cached(root, entity_type, id) {
            Ok(Some(body)) => {
                info!(target: "ce_render", entity_type, id, "render: cache hit");
                return Ok(body);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(target: "ce_render", entity_type, id, error=%e, "render: cache read failed; regenerating");
            }
        }
    }
    // Build the FULL depth-1 context up front. The model's job is to format,
    // not to decide what's relevant — we hand it everything (every fact,
    // alias, outgoing ref + target's facts, incoming ref + source's facts)
    // so nothing is silently dropped.
    // Build the full depth-1 context once and hand it to the model in a
    // single call. We already know the task (overview rendering) and we've
    // already pre-fetched everything the model would have asked for via
    // tools, so giving it tool access only burns latency.
    let seed = {
        let store = Store::open(store_path).map_err(|e| ApiError::Internal(format!("store: {e}")))?;
        build_full_context(&store, entity_type, id)?
    };
    info!(target: "ce_render", entity_type, id, seed_chars = seed.len(), "render: full depth-1 context built");

    let system = system_prompt();
    let body = json!({
        "systemInstruction": { "parts": [{ "text": system }] },
        "contents": [{ "role": "user", "parts": [{ "text": seed }] }],
        "generationConfig": { "temperature": 0.0 },
    });

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs.max(60)))
        .build()
        .map_err(|e| ApiError::Internal(format!("http build: {e}")))?;
    let url = format!(
        "{}/models/{}:generateContent?key={}",
        cfg.effective_base_url().trim_end_matches('/'),
        cfg.model,
        cfg.effective_api_key().map_err(|e| ApiError::Internal(e.to_string()))?,
    );

    info!(target: "ce_render", entity_type, id, "render: → gemini (single-pass)");
    // Heartbeat so the operator knows the call is still alive on long inputs.
    let done_flag = Arc::new(AtomicBool::new(false));
    let hb_flag = done_flag.clone();
    let hb_entity = format!("{}/{}", entity_type, id);
    let hb_started = std::time::Instant::now();
    tokio::spawn(async move {
        let mut elapsed = 0u64;
        while !hb_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if hb_flag.load(Ordering::Relaxed) { break; }
            elapsed = hb_started.elapsed().as_secs();
            info!(target: "ce_render", entity = %hb_entity, secs = elapsed, "render: …still waiting on gemini");
        }
    });
    let resp = http.post(&url).json(&body).send().await
        .map_err(|e| { done_flag.store(true, Ordering::Relaxed); ApiError::Internal(format!("gemini: {e}")) })?;
    done_flag.store(true, Ordering::Relaxed);
    let status = resp.status();
    let text = resp.text().await
        .map_err(|e| ApiError::Internal(format!("gemini body: {e}")))?;
    if !status.is_success() {
        return Err(ApiError::Internal(format!("gemini {status}: {text}")));
    }
    let parsed: GemResp = serde_json::from_str(&text)
        .map_err(|e| ApiError::Internal(format!("gemini parse: {e}")))?;
    if let Some(err) = parsed.error {
        return Err(ApiError::Internal(format!("gemini api: {}", err.message)));
    }
    let cand = parsed.candidates.into_iter().next()
        .ok_or_else(|| ApiError::Internal("gemini: no candidates".into()))?;

    let mut out_text = String::new();
    for part in &cand.content.parts {
        if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
            out_text.push_str(t);
        }
    }
    let trimmed = out_text.trim();
    if trimmed.is_empty() {
        return Err(ApiError::Internal(format!(
            "render: model returned no text for {}/{}", entity_type, id
        )));
    }
    info!(target: "ce_render", entity_type, id, chars = trimmed.len(), "render: ✓ done");
    let body = trimmed.to_string();
    if let Some(root) = md_store_root {
        if let Err(e) = crate::md_cache::write_cached(root, entity_type, id, &body) {
            tracing::warn!(target: "ce_render", entity_type, id, error=%e, "render: cache write failed");
        }
    }
    Ok(body)
}

/// Hard cap on a single fact value's chars. Long bodies (email/chat
/// transcripts) get a head + tail preview with `…<N chars elided>…` between
/// — predicate is still surfaced, value is still recognizable, but the model
/// doesn't drown in 5KB transcripts × 50 neighbors.
const MAX_VALUE_CHARS: usize = 500;
/// Per-neighbor fact ceiling. Above this, list a count and let the user
/// click through to the neighbor's own /render.md.
const MAX_NEIGHBOR_FACTS: usize = 40;

fn shorten_value(v: &str) -> String {
    if v.chars().count() <= MAX_VALUE_CHARS {
        return v.to_string();
    }
    let head_chars = MAX_VALUE_CHARS / 2;
    let tail_chars = MAX_VALUE_CHARS / 2;
    let total = v.chars().count();
    let head: String = v.chars().take(head_chars).collect();
    let tail: String = v.chars().skip(total - tail_chars).collect();
    format!("{}…[{} chars elided]…{}", head.trim_end(), total - head_chars - tail_chars, tail.trim_start())
}

/// Pull the entity AND every depth-1 neighbor in both directions, with full
/// fact lists for each. The result is a structured text dump the model
/// reformats — nothing is dropped because "deemed unnecessary".
fn build_full_context(
    store: &Store,
    entity_type: &str,
    id: &str,
) -> Result<String, ApiError> {
    let label = store
        .entity_labels_bulk(&[id.to_string()])
        .map_err(|e| ApiError::Internal(format!("labels: {e}")))?
        .remove(id)
        .unwrap_or_default();
    let aliases = store
        .aliases_for_subject(id)
        .map_err(|e| ApiError::Internal(format!("aliases: {e}")))?;
    let facts = store
        .facts_for_subject(id)
        .map_err(|e| ApiError::Internal(format!("facts: {e}")))?;
    let incoming = store
        .incoming_refs(id)
        .map_err(|e| ApiError::Internal(format!("incoming: {e}")))?;

    if facts.is_empty() && incoming.is_empty() {
        let exists = store
            .all_entities()
            .map_err(|e| ApiError::Internal(format!("entities: {e}")))?
            .iter()
            .any(|(eid, et)| eid == id && et == entity_type);
        if !exists { return Err(ApiError::NotFound); }
    }

    // Collect every neighbor id (outgoing ref targets ∪ incoming subjects).
    let mut neighbor_ids: BTreeSet<String> = BTreeSet::new();
    for f in &facts {
        if f.predicate.starts_with("ref:") {
            if let Ok(v) = serde_json::from_str::<Value>(&f.object_json) {
                if let Some(s) = v.as_str() {
                    if s != id { neighbor_ids.insert(s.to_string()); }
                }
            }
        }
    }
    for (_, subj, _, _, _) in &incoming {
        if subj != id { neighbor_ids.insert(subj.clone()); }
    }

    // Bulk lookups so we don't hit the DB N times.
    let neighbor_id_vec: Vec<String> = neighbor_ids.iter().cloned().collect();
    let neighbor_labels = store
        .entity_labels_bulk(&neighbor_id_vec)
        .map_err(|e| ApiError::Internal(format!("neighbor labels: {e}")))?;
    // Pre-fetch every neighbor's aliases too. Saves the model from asking.
    let mut neighbor_aliases: HashMap<String, Vec<(String, String, f64)>> = HashMap::new();
    for nid in &neighbor_id_vec {
        let a = store.aliases_for_subject(nid)
            .map_err(|e| ApiError::Internal(format!("neighbor aliases: {e}")))?;
        if !a.is_empty() { neighbor_aliases.insert(nid.clone(), a); }
    }

    let mut out = String::new();
    out.push_str(&format!(
        "You are rendering one entity from a knowledge graph as Markdown. \
        Below is its complete depth-1 context: every fact, alias, outgoing edge target (with that target's full facts), and incoming edge source (with that source's full facts). \
        Render ALL of it. Do not summarize. Do not skip facts because they look redundant. Do not omit a neighbor because there are many. \
        Format faithfully: every neighbor gets its own subsection. Every fact appears under its neighbor.\n\n",
    ));
    out.push_str(&format!("# SUBJECT\n"));
    out.push_str(&format!("entity_type: {}\nid: {}\n", entity_type, id));
    if !label.is_empty() { out.push_str(&format!("label: {}\n", label)); }

    out.push_str(&format!("\n## ALIASES ({})\n", aliases.len()));
    for (alias, source, conf) in &aliases {
        out.push_str(&format!("- `{}` (source={}, conf={:.2})\n", alias, source, conf));
    }

    out.push_str(&format!("\n## FACTS — attributes & outgoing refs ({})\n", facts.len()));
    for f in &facts {
        out.push_str(&format!(
            "- `{}` = {} [adapter={} conf={:.2}{}]\n",
            f.predicate,
            shorten_value(&f.object_json),
            f.adapter, f.confidence,
            f.locator.as_ref().map(|l| format!(" loc={}", l)).unwrap_or_default(),
        ));
    }

    out.push_str(&format!("\n## INCOMING REFS — back-edges ({})\n", incoming.len()));
    for (pred, subj, subj_type, adapter, conf) in &incoming {
        let lbl = neighbor_labels.get(subj).map(String::as_str).unwrap_or("");
        out.push_str(&format!(
            "- `{}` ← {}/{}{}{}  [adapter={} conf={:.2}]\n",
            pred, subj_type, subj,
            if lbl.is_empty() { "" } else { " — " },
            lbl,
            adapter, conf,
        ));
    }

    out.push_str(&format!("\n## NEIGHBORS DEPTH=1 — full facts of every connected entity ({})\n", neighbor_ids.len()));
    for nid in &neighbor_id_vec {
        let ntype = store.entity_type_of(nid)
            .map_err(|e| ApiError::Internal(format!("type: {e}")))?
            .unwrap_or_default();
        let nfacts = store.facts_for_subject(nid)
            .map_err(|e| ApiError::Internal(format!("neighbor facts: {e}")))?;
        let nlabel = neighbor_labels.get(nid).cloned().unwrap_or_default();
        out.push_str(&format!(
            "\n### {}/{}{}\n",
            ntype, nid,
            if nlabel.is_empty() { String::new() } else { format!(" — {}", nlabel) },
        ));
        if let Some(als) = neighbor_aliases.get(nid) {
            let names: Vec<String> = als.iter().take(6).map(|(a, _, _)| a.clone()).collect();
            if !names.is_empty() {
                out.push_str(&format!("aliases: {}\n", names.join(", ")));
            }
        }
        if nfacts.is_empty() {
            out.push_str("_(no facts recorded)_\n");
        } else {
            let total = nfacts.len();
            for f in nfacts.iter().take(MAX_NEIGHBOR_FACTS) {
                out.push_str(&format!("- `{}` = {}\n", f.predicate, shorten_value(&f.object_json)));
            }
            if total > MAX_NEIGHBOR_FACTS {
                out.push_str(&format!(
                    "- _(…{} more facts on this neighbor — see /entities/{}/{})_\n",
                    total - MAX_NEIGHBOR_FACTS, ntype, nid,
                ));
            }
        }
    }

    Ok(out)
}

fn system_prompt() -> &'static str {
    "You are a faithful Markdown formatter for a knowledge-graph entity. \
     \
     CONTRACT — non-negotiable: \
     1. The user message contains the COMPLETE depth-1 context for one entity: every fact, every alias, every outgoing edge target with that target's full facts, every incoming edge source with that source's full facts. \
     2. Output every fact in the input. Do not summarize, paraphrase, group-as-'and others', or drop a value because it looks redundant or low-value. \
     3. Do not invent any predicate, value, neighbor, or alias not present in the input. \
     4. Each neighbor in the NEIGHBORS section becomes its own subsection in the output, with all of its facts listed. Do not collapse multiple neighbors. \
     5. Cross-link every neighbor id as a Markdown link to `/entities/<type>/<id>`. \
     \
     The graph is bidirectional — outgoing and incoming refs both appear in the input and both must appear in the output (separate sections). \
     \
     Output: ONE Markdown document, no code fences around the whole thing, no preamble, no apology if the input is large. Just the rendered profile."
}

#[allow(dead_code)]
fn _unused_tool_block() {} // (tool-calling removed — render is single-pass; depth-1 context is exhaustive)
#[cfg(any())]
fn tool_declarations() -> Vec<Value> {
    vec![
        json!({
            "name": "neighbors",
            "description": "Bidirectional neighborhood of an entity in one call: attributes + outgoing refs + incoming refs. Prefer this over get_facts/get_incoming when expanding.",
            "parameters": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
            }
        }),
        json!({
            "name": "get_facts",
            "description": "All facts for an entity id. Returns predicate / value / adapter / confidence.",
            "parameters": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
            }
        }),
        json!({
            "name": "get_incoming",
            "description": "Other entities that reference this id via ref:* predicates. Use to populate a 'Referenced by' section.",
            "parameters": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
            }
        }),
        json!({
            "name": "get_aliases",
            "description": "Known alternate names / surface forms of an entity.",
            "parameters": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
            }
        }),
        json!({
            "name": "search",
            "description": "BM25 keyword search over all entities. Use when looking up a name mentioned in an attribute that wasn't yet linked.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "k": { "type": "integer" },
                },
                "required": ["query"],
            }
        }),
    ]
}

#[cfg(any())]
fn execute_tool(store: &Store, name: &str, args: &Value) -> Result<Value, String> {
    match name {
        "neighbors" => {
            let id = args.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
            let etype = store.entity_type_of(id)
                .map_err(|e| format!("type: {e}"))?
                .unwrap_or_default();
            let label = store.entity_labels_bulk(&[id.to_string()])
                .map_err(|e| format!("labels: {e}"))?
                .remove(id);
            let facts = store.facts_for_subject(id).map_err(|e| format!("facts: {e}"))?;
            let mut attributes: Vec<Value> = Vec::new();
            let mut outgoing: Vec<Value> = Vec::new();
            let ref_targets: Vec<String> = facts.iter()
                .filter(|f| f.predicate.starts_with("ref:"))
                .filter_map(|f| serde_json::from_str::<Value>(&f.object_json).ok()
                    .and_then(|v| v.as_str().map(|s| s.to_string())))
                .collect();
            let target_labels = store.entity_labels_bulk(&ref_targets)
                .map_err(|e| format!("labels: {e}"))?;
            for f in facts.into_iter().take(300) {
                if let Some(target_type) = f.predicate.strip_prefix("ref:") {
                    let tid = serde_json::from_str::<Value>(&f.object_json).ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    let tlabel = target_labels.get(&tid).cloned();
                    outgoing.push(json!({
                        "predicate": f.predicate,
                        "target_id": tid,
                        "target_type": target_type,
                        "target_label": tlabel,
                    }));
                } else {
                    attributes.push(json!({
                        "predicate": f.predicate,
                        "value": f.object_json,
                    }));
                }
            }
            let in_rows = store.incoming_refs(id).map_err(|e| format!("incoming: {e}"))?;
            let in_ids: Vec<String> = in_rows.iter().map(|(_, s, _, _, _)| s.clone()).collect();
            let in_labels = store.entity_labels_bulk(&in_ids)
                .map_err(|e| format!("labels: {e}"))?;
            let total_in = in_rows.len();
            let incoming: Vec<Value> = in_rows.into_iter().take(100).map(|(predicate, subject, subject_type, _adapter, _confidence)| {
                let subject_label = in_labels.get(&subject).cloned();
                json!({
                    "predicate": predicate,
                    "from_id": subject,
                    "from_type": subject_type,
                    "from_label": subject_label,
                })
            }).collect();
            Ok(json!({
                "id": id,
                "entity_type": etype,
                "label": label,
                "attributes": attributes,
                "outgoing": outgoing,
                "incoming": incoming,
                "total_incoming": total_in,
            }))
        }
        "get_facts" => {
            let id = args.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
            let facts = store.facts_for_subject(id)
                .map_err(|e| format!("facts: {e}"))?;
            let out: Vec<Value> = facts.into_iter().take(200).map(|f| json!({
                "predicate": f.predicate,
                "object": f.object_json,
                "adapter": f.adapter,
                "confidence": f.confidence,
                "locator": f.locator,
            })).collect();
            Ok(json!({ "facts": out }))
        }
        "get_incoming" => {
            let id = args.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
            let rows = store.incoming_refs(id)
                .map_err(|e| format!("incoming: {e}"))?;
            let labels = store.entity_labels_bulk(
                &rows.iter().map(|(_, s, _, _, _)| s.clone()).collect::<Vec<_>>()
            ).map_err(|e| format!("labels: {e}"))?;
            let out: Vec<Value> = rows.into_iter().take(100).map(|(predicate, subject, subject_type, adapter, confidence)| {
                let label = labels.get(&subject).cloned();
                json!({
                    "predicate": predicate,
                    "subject": subject,
                    "subject_type": subject_type,
                    "subject_label": label,
                    "adapter": adapter,
                    "confidence": confidence,
                })
            }).collect();
            Ok(json!({ "incoming": out }))
        }
        "get_aliases" => {
            let id = args.get("id").and_then(|v| v.as_str()).ok_or("missing id")?;
            let aliases = store.aliases_for_subject(id)
                .map_err(|e| format!("aliases: {e}"))?;
            let out: Vec<Value> = aliases.into_iter().map(|(alias, source, conf)| json!({
                "alias": alias,
                "source": source,
                "confidence": conf,
            })).collect();
            Ok(json!({ "aliases": out }))
        }
        "search" => {
            // BM25 search requires an index path which the API state owns.
            // For now, return a not-supported marker; wire the index when
            // /retrieve already does.
            let _ = args;
            Ok(json!({ "error": "search tool requires an index; not wired in this build" }))
        }
        _ => Err(format!("unknown tool: {}", name)),
    }
}

#[cfg(any())]
fn summarize_tool_result(v: &Value) -> String {
    if let Value::Object(map) = v {
        let mut parts: Vec<String> = Vec::new();
        for (k, val) in map {
            let snippet = match val {
                Value::Array(a) => format!("{}=[{} items]", k, a.len()),
                Value::Number(n) => format!("{}={}", k, n),
                Value::String(s) => {
                    let prev: String = s.chars().take(80).collect();
                    format!("{}={}", k, prev.replace('\n', " ⏎ "))
                }
                Value::Bool(b) => format!("{}={}", k, b),
                Value::Null => continue,
                Value::Object(_) => format!("{}={{...}}", k),
            };
            parts.push(snippet);
        }
        return parts.join(" ");
    }
    let s = v.to_string();
    if s.len() <= 200 { s } else { format!("{}…", &s[..200]) }
}

#[cfg(any())]
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max { return s.to_string(); }
    let mut end = max;
    while !s.is_char_boundary(end) { end -= 1; }
    format!("{}…", &s[..end])
}

// ---- Gemini response shape (only the bits we read) ----

#[derive(Deserialize)]
struct GemResp {
    #[serde(default)]
    candidates: Vec<GemCand>,
    #[serde(default)]
    error: Option<GemErr>,
}
#[derive(Deserialize)]
struct GemErr { message: String }
#[derive(Deserialize)]
struct GemCand { content: GemContent }
#[derive(Deserialize)]
struct GemContent {
    #[serde(default)]
    parts: Vec<Value>,
}
