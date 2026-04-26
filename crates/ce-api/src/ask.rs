//! Semantic Q&A over the fact graph. Same tool-calling pattern as
//! `render.rs`: the model walks the graph through a small toolbox until it
//! has enough context to answer, then returns Markdown.

use std::path::Path as FsPath;
use std::time::Duration;

use ce_llm::LlmConfig;
use ce_store::Store;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::ApiError;

const MAX_TURNS: usize = 12;

pub async fn answer_question(
    store_path: &FsPath,
    idx_path: Option<&FsPath>,
    cfg: &LlmConfig,
    question: &str,
) -> Result<String, ApiError> {
    let system = system_prompt();
    let tool_defs = tool_declarations(idx_path.is_some());

    info!(target: "ce_ask", q = %question, "ask: question received");
    let mut contents: Vec<Value> = vec![json!({
        "role": "user",
        "parts": [{ "text": format!(
            "Question: {question}\n\n\
             Walk the graph with tools, then write a Markdown answer in prose using the actual \
             attribute values you fetched (names, titles, descriptions, dates, amounts) — not raw ids. \
             End with a small `Sources:` list (id · type · label) for verifiability. \
             Aim for 2–3 round-trips: locate → batched neighbors → answer."
        ) }],
    })];

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

    for turn in 0..MAX_TURNS {
        let body = json!({
            "systemInstruction": { "parts": [{ "text": system }] },
            "contents": contents,
            "tools": [{ "functionDeclarations": tool_defs.clone() }],
            "generationConfig": { "temperature": 0.0 },
        });

        info!(target: "ce_ask", turn, "ask: → gemini");
        let resp = http.post(&url).json(&body).send().await
            .map_err(|e| ApiError::Internal(format!("gemini: {e}")))?;
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

        contents.push(json!({ "role": "model", "parts": cand.content.parts }));

        let mut tool_calls: Vec<(String, Value)> = Vec::new();
        let mut final_text: Option<String> = None;
        for part in &cand.content.parts {
            if let Some(fc) = part.get("functionCall") {
                let name = fc.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let args = fc.get("args").cloned().unwrap_or(json!({}));
                info!(target: "ce_ask", turn, tool = %name, args = %args, "ask: ← tool call");
                tool_calls.push((name, args));
            } else if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                let trimmed = t.trim();
                if !trimmed.is_empty() {
                    info!(target: "ce_ask", turn, text = %truncate_for_log(trimmed, 600), "ask: ← model text");
                    final_text = Some(trimmed.to_string());
                }
            }
        }

        if tool_calls.is_empty() {
            info!(target: "ce_ask", turns = turn + 1, "ask: ✓ done");
            return Ok(final_text.unwrap_or_else(|| "_(no answer produced)_".to_string()));
        }

        let mut response_parts: Vec<Value> = Vec::new();
        {
            let store = Store::open(store_path)
                .map_err(|e| ApiError::Internal(format!("store: {e}")))?;
            for (name, args) in tool_calls {
                let result = execute_tool(&store, idx_path, &name, &args)
                    .unwrap_or_else(|e| json!({ "error": e.to_string() }));
                let summary = summarize_tool_result(&result);
                info!(target: "ce_ask", turn, tool = %name, result = %summary, "ask: → tool result");
                response_parts.push(json!({
                    "functionResponse": {
                        "name": name,
                        "response": { "content": result },
                    }
                }));
            }
        }
        contents.push(json!({ "role": "user", "parts": response_parts }));
    }

    warn!("ask: hit MAX_TURNS without answer");
    Err(ApiError::Internal(format!(
        "ask: exceeded {} turns of tool-calling",
        MAX_TURNS,
    )))
}

fn system_prompt() -> &'static str {
    "You answer questions about an enterprise knowledge graph by walking it through tool calls, \
     then synthesizing a real answer from the data you fetched. \
     \
     ━━━ ANSWER STYLE (most common failure mode — read carefully) ━━━ \
     The answer is PROSE built from the attribute values you fetched, not a list of ids. \
     A `neighbors` call returns `attributes` (e.g. name, title, email, description, body, amount, date) — \
     USE THOSE VALUES in the answer. Refer to entities by their human label/name, never by raw id. \
     \
     Bad:  'Customer C-101 has 3 orders linked via ref:orders to O-77, O-92, O-103.' \
     Good: 'Acme Corp has placed three orders since March — a $4.2k batch of bearings (Mar 4), \
            a follow-up restock (Apr 1), and a custom seal order (Apr 19).' \
     \
     End with a small `Sources:` footnote — one bullet per entity actually used (id · type · label). \
     That is the ONLY place raw ids appear. The body is prose. \
     \
     ━━━ CALL BUDGET (target: 2–3 round-trips, hard ceiling 12) ━━━ \
     1. LOCATE — one turn. `search(query)` if available, else `list_entities_of_type` or pull the id straight from the question. \
     2. EXPAND — one turn, BATCHED. Pass every id you care about as a single `neighbors(ids: [...])` call. \
        Emit parallel function calls in the same turn when you need multiple tools (Gemini supports this). \
     3. ANSWER — write the prose. Only fetch more if a specific attribute is missing for the answer; \
        when you do, batch again — never call a tool once per id. \
     \
     Anti-patterns to avoid: \
     - calling `neighbors(ids: [\"x\"])` then `neighbors(ids: [\"y\"])` — collapse into `neighbors(ids: [\"x\",\"y\"])`. \
     - fetching `get_facts` after `neighbors` for the same ids (neighbors already includes them). \
     - looping over incoming refs one id at a time — pass the whole set. \
     \
     ━━━ GRAPH MECHANICS ━━━ \
     Every edge is bidirectional. `(A, ref:customers, B)` means BOTH 'A points at customer B' AND 'B is referenced by A'. \
     - 'what does X reference' → outgoing → `neighbors`. \
     - 'what references X' (reviews of product, orders by customer, emails to employee) → incoming → `neighbors` (it returns both directions). \
     - Aggregations: `count_predicate(target, predicate)` for 'how many', `top_values_by_subject(predicate)` for 'which one most'. \
     \
     ━━━ TOOLS ━━━ \
     - `neighbors(ids)` — attributes + outgoing + incoming for many ids in one call. PREFERRED expansion tool. \
     - `get_facts(ids)` / `get_incoming(ids)` / `get_aliases(ids)` — narrower variants of neighbors; use only when neighbors would return too much. All take id LISTS. \
     - `search(query)` — BM25 full-text, only when an index is built. If absent, use `list_entities_of_type`. \
     - `list_entity_types` — discover types when the question is vague. \
     - `list_entities_of_type(type, contains?)` — browse + filter ids by type. \
     - `count_predicate` / `top_values_by_subject` — aggregations. \
     \
     ━━━ HONESTY ━━━ \
     Do not invent ids, facts, or attribute values. If the graph genuinely lacks the answer, say so plainly. \
     Never refuse a question because one tool is unavailable — fall back to another."
}

fn tool_declarations(have_index: bool) -> Vec<Value> {
    let mut tools = vec![
        json!({
            "name": "neighbors",
            "description": "Bidirectional neighborhood of one OR MANY entities in one call. Pass `ids` as an array — even a single id should be wrapped `[\"x\"]`. Returns one object per id with attributes, outgoing refs, and incoming refs. Prefer this over get_facts when expanding nodes — fewer round-trips.",
            "parameters": {
                "type": "object",
                "properties": { "ids": { "type": "array", "items": { "type": "string" } } },
                "required": ["ids"],
            }
        }),
        json!({
            "name": "get_facts",
            "description": "Facts (attributes + outgoing refs) for one OR MANY entities. Pass `ids` as an array. Returns a map id → facts.",
            "parameters": {
                "type": "object",
                "properties": { "ids": { "type": "array", "items": { "type": "string" } } },
                "required": ["ids"],
            }
        }),
        json!({
            "name": "get_incoming",
            "description": "Back-edges for one OR MANY entities. Returns a map id → incoming refs (subjects that point AT each id).",
            "parameters": {
                "type": "object",
                "properties": { "ids": { "type": "array", "items": { "type": "string" } } },
                "required": ["ids"],
            }
        }),
        json!({
            "name": "get_aliases",
            "description": "Known surface forms for one OR MANY entities. Returns a map id → aliases.",
            "parameters": {
                "type": "object",
                "properties": { "ids": { "type": "array", "items": { "type": "string" } } },
                "required": ["ids"],
            }
        }),
        json!({
            "name": "list_entity_types",
            "description": "Distinct entity types in the graph and their counts. Call this first if you don't know what types exist.",
            "parameters": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "list_entities_of_type",
            "description": "List entity ids of a given type, optionally filtered by id-substring.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_type": { "type": "string" },
                    "contains": { "type": "string" },
                    "limit": { "type": "integer" },
                },
                "required": ["entity_type"],
            }
        }),
        json!({
            "name": "count_predicate",
            "description": "Count how many distinct subjects link TO a target id via a given predicate. e.g. count_predicate(target='B07JW9H4J1', predicate='ref:product_sentiment') answers 'how many sentiment records does product X have'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target": { "type": "string" },
                    "predicate": { "type": "string" },
                },
                "required": ["target", "predicate"],
            }
        }),
        json!({
            "name": "top_values_by_subject",
            "description": "Group facts by predicate's object value; return the most common values. e.g. top_values_by_subject(predicate='ref:customers') answers 'which customer is referenced most often'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "predicate": { "type": "string" },
                    "limit": { "type": "integer" },
                },
                "required": ["predicate"],
            }
        }),
    ];
    if have_index {
        tools.push(json!({
            "name": "search",
            "description": "BM25 keyword search across all entities. Returns entity ids + types ranked by match. Use when looking for entities by name, product, topic, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "k": { "type": "integer" },
                },
                "required": ["query"],
            }
        }));
    }
    tools
}

fn execute_tool(
    store: &Store,
    idx_path: Option<&FsPath>,
    name: &str,
    args: &Value,
) -> Result<Value, String> {
    match name {
        "neighbors" => {
            let ids = parse_id_list(args)?;
            let mut results: Vec<Value> = Vec::with_capacity(ids.len());
            for id in &ids {
                results.push(neighbors_for(store, id)?);
            }
            Ok(json!({ "results": results }))
        }
        "get_facts" => {
            let ids = parse_id_list(args)?;
            let mut results: Vec<Value> = Vec::with_capacity(ids.len());
            for id in &ids {
                let facts = store.facts_for_subject(id).map_err(|e| format!("facts: {e}"))?;
                let etype = store.entity_type_of(id)
                    .map_err(|e| format!("type: {e}"))?
                    .unwrap_or_default();
                let out: Vec<Value> = facts.into_iter().take(200).map(|f| json!({
                    "predicate": f.predicate,
                    "object": f.object_json,
                    "adapter": f.adapter,
                    "confidence": f.confidence,
                })).collect();
                results.push(json!({ "id": id, "entity_type": etype, "facts": out }));
            }
            Ok(json!({ "results": results }))
        }
        "get_incoming" => {
            let ids = parse_id_list(args)?;
            let mut results: Vec<Value> = Vec::with_capacity(ids.len());
            for id in &ids {
                let rows = store.incoming_refs(id).map_err(|e| format!("incoming: {e}"))?;
                let labels = store.entity_labels_bulk(
                    &rows.iter().map(|(_, s, _, _, _)| s.clone()).collect::<Vec<_>>()
                ).map_err(|e| format!("labels: {e}"))?;
                let total = rows.len();
                let out: Vec<Value> = rows.into_iter().take(100).map(|(predicate, subject, subject_type, _adapter, confidence)| {
                    let label = labels.get(&subject).cloned();
                    json!({
                        "predicate": predicate,
                        "subject": subject,
                        "subject_type": subject_type,
                        "subject_label": label,
                        "confidence": confidence,
                    })
                }).collect();
                results.push(json!({ "id": id, "total_incoming": total, "showing": out.len(), "incoming": out }));
            }
            Ok(json!({ "results": results }))
        }
        "get_aliases" => {
            let ids = parse_id_list(args)?;
            let mut results: Vec<Value> = Vec::with_capacity(ids.len());
            for id in &ids {
                let aliases = store.aliases_for_subject(id).map_err(|e| format!("aliases: {e}"))?;
                let out: Vec<Value> = aliases.into_iter().map(|(alias, source, conf)| json!({
                    "alias": alias,
                    "source": source,
                    "confidence": conf,
                })).collect();
                results.push(json!({ "id": id, "aliases": out }));
            }
            Ok(json!({ "results": results }))
        }
        "list_entity_types" => {
            let counts = store.entity_counts().map_err(|e| format!("counts: {e}"))?;
            let out: Vec<Value> = counts.into_iter()
                .map(|(t, c)| json!({ "entity_type": t, "count": c }))
                .collect();
            Ok(json!({ "types": out }))
        }
        "list_entities_of_type" => {
            let etype = args.get("entity_type").and_then(|v| v.as_str())
                .ok_or("missing entity_type")?;
            let contains = args.get("contains").and_then(|v| v.as_str());
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
            let (ids, total) = store.entities_of_type(etype, contains, 0, limit.min(500))
                .map_err(|e| format!("list: {e}"))?;
            let labels = store.entity_labels_bulk(&ids)
                .map_err(|e| format!("labels: {e}"))?;
            let items: Vec<Value> = ids.into_iter().map(|id| {
                let label = labels.get(&id).cloned();
                json!({ "id": id, "label": label })
            }).collect();
            Ok(json!({ "entity_type": etype, "total": total, "items": items }))
        }
        "count_predicate" => {
            let target = args.get("target").and_then(|v| v.as_str()).ok_or("missing target")?;
            let predicate = args.get("predicate").and_then(|v| v.as_str()).ok_or("missing predicate")?;
            // Reuse incoming_refs for ref:* predicates; otherwise fall back to
            // exact-object counting.
            if predicate.starts_with("ref:") {
                let rows = store.incoming_refs(target)
                    .map_err(|e| format!("incoming: {e}"))?;
                let n = rows.iter().filter(|(p, _, _, _, _)| p == predicate).count();
                return Ok(json!({ "target": target, "predicate": predicate, "count": n }));
            }
            // Generic fallback: count distinct subjects whose `object_json` equals the target.
            let n = store.count_facts_with_object(predicate, target)
                .map_err(|e| format!("count: {e}"))?;
            Ok(json!({ "target": target, "predicate": predicate, "count": n }))
        }
        "top_values_by_subject" => {
            let predicate = args.get("predicate").and_then(|v| v.as_str()).ok_or("missing predicate")?;
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
            let rows = store.top_values(predicate, limit.min(100))
                .map_err(|e| format!("top: {e}"))?;
            let out: Vec<Value> = rows.into_iter()
                .map(|(value, count)| json!({ "value": value, "count": count }))
                .collect();
            Ok(json!({ "predicate": predicate, "top": out }))
        }
        "search" => {
            let idx = idx_path.ok_or("search: server has no index")?;
            let query = args.get("query").and_then(|v| v.as_str()).ok_or("missing query")?;
            let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(10).min(50) as usize;
            let hits = ce_search::search(idx, query, k)
                .map_err(|e| format!("search: {e}"))?;
            let labels = store.entity_labels_bulk(
                &hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>()
            ).map_err(|e| format!("labels: {e}"))?;
            let out: Vec<Value> = hits.into_iter().map(|h| {
                let label = labels.get(&h.id).cloned();
                json!({
                    "id": h.id,
                    "entity_type": h.entity_type,
                    "label": label,
                    "score": h.score,
                })
            }).collect();
            Ok(json!({ "query": query, "hits": out }))
        }
        _ => Err(format!("unknown tool: {}", name)),
    }
}

/// Accept either `{ids: [...]}` (preferred — batch) or `{id: "..."}` (legacy
/// single-id, kept so the model doesn't fail when it slips into old shape).
fn parse_id_list(args: &Value) -> Result<Vec<String>, String> {
    if let Some(arr) = args.get("ids").and_then(|v| v.as_array()) {
        let out: Vec<String> = arr.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        if out.is_empty() { return Err("ids: empty list".into()); }
        return Ok(out);
    }
    if let Some(s) = args.get("id").and_then(|v| v.as_str()) {
        return Ok(vec![s.to_string()]);
    }
    Err("missing ids[]".into())
}

fn neighbors_for(store: &Store, id: &str) -> Result<Value, String> {
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

fn truncate_for_log(s: &str, max: usize) -> String {
    if s.chars().count() <= max { return s.replace('\n', " ⏎ "); }
    let truncated: String = s.chars().take(max).collect();
    format!("{}…", truncated.replace('\n', " ⏎ "))
}

/// Compress a tool result so the log line stays readable. Pulls counts out
/// of arrays / total fields so the human watching the console can see
/// "got 13 incoming refs" instead of dumping the whole list.
fn summarize_tool_result(v: &Value) -> String {
    if let Value::Object(map) = v {
        let mut parts: Vec<String> = Vec::new();
        for (k, val) in map {
            let snippet = match val {
                Value::Array(a) => format!("{}=[{} items]", k, a.len()),
                Value::Number(n) => format!("{}={}", k, n),
                Value::String(s) => format!("{}={}", k, truncate_for_log(s, 80)),
                Value::Bool(b) => format!("{}={}", k, b),
                Value::Null => continue,
                Value::Object(_) => format!("{}={{...}}", k),
            };
            parts.push(snippet);
        }
        return parts.join(" ");
    }
    truncate_for_log(&v.to_string(), 200)
}

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
