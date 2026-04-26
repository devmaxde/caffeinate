//! Phase C — embedding pass.
//!
//! For every entity we synthesise a "card text" (type, id, top aliases, top
//! short attribute facts), hash it, and embed when the hash differs from the
//! one stored alongside the previous embedding. Generic ranking: by alias
//! confidence and by value length. No predicate names are hardcoded.

use std::collections::HashMap;

use ce_llm::{EmbedClient, LlmError};
use ce_store::{Store, StoreError};
use rayon::prelude::*;
use serde_json::Value;

#[derive(Debug, Default)]
pub struct EmbedStats {
    pub considered: usize,
    pub embedded: usize,
    pub skipped_unchanged: usize,
    pub batches: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("llm: {0}")]
    Llm(#[from] LlmError),
    #[error("dim mismatch: model={model} got={got} expected={expected}")]
    Dim { model: String, got: usize, expected: usize },
}

const CARD_LINE_MAX: usize = 200;
const CARD_TOTAL_MAX: usize = 2048;
const TOP_ALIASES: usize = 5;
const TOP_FACTS: usize = 5;

/// Build a card-text for one entity. Format:
/// ```text
/// {entity_type} {id}
/// aliases: a1, a2, a3
/// {pred1}: {val1}
/// ...
/// ```
/// `aliases` and `facts` are pre-fetched. Aliases are ranked by confidence,
/// facts by short-value-first. Truncates per-line and total length.
pub fn build_card_text(
    entity_id: &str,
    entity_type: &str,
    aliases: &[(String, f64)],
    facts: &[(String, String)],
) -> String {
    let mut out = String::new();
    out.push_str(entity_type);
    out.push(' ');
    out.push_str(entity_id);

    if !aliases.is_empty() {
        let mut sorted = aliases.to_vec();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen: std::collections::HashSet<String> = Default::default();
        let mut picked: Vec<&str> = Vec::new();
        for (a, _c) in &sorted {
            let lk = a.to_lowercase();
            if !seen.insert(lk) { continue; }
            if a.eq_ignore_ascii_case(entity_id) { continue; }
            picked.push(a);
            if picked.len() >= TOP_ALIASES { break; }
        }
        if !picked.is_empty() {
            out.push_str("\naliases: ");
            out.push_str(&picked.join(", "));
        }
    }

    if !facts.is_empty() {
        let mut sorted = facts.to_vec();
        // Short values first → identifying attributes float to the top, body
        // text sinks to the bottom (and gets truncated out).
        sorted.sort_by_key(|(_, v)| v.chars().count());
        for (pred, val) in sorted.into_iter().take(TOP_FACTS) {
            let line = format!("\n{}: {}", pred, truncate(&val, CARD_LINE_MAX));
            out.push_str(&line);
        }
    }
    truncate(&out, CARD_TOTAL_MAX)
}

pub fn card_hash(card_text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(card_text.as_bytes());
    hex::encode(h.finalize())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max { return s.to_string(); }
    let mut out = String::new();
    for c in s.chars() {
        if out.chars().count() >= max { break; }
        out.push(c);
    }
    out.push('…');
    out
}

/// Pull a printable string from a JSON-encoded object. Strings come through as
/// their value; numbers and booleans get stringified. Arrays/objects are
/// rendered as their JSON form (kept short by the caller's truncation).
fn obj_to_display(obj_json: &str) -> Option<String> {
    let v: Value = serde_json::from_str(obj_json).ok()?;
    match v {
        Value::String(s) => Some(s),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Run an embedding pass over all entities. Diffs by `card_hash` AND by
/// `model` so a provider switch transparently triggers re-embed.
pub async fn run_entity_embed_pass(
    store: &mut Store,
    embed: &dyn EmbedClient,
    batch_size: usize,
    force: bool,
    when: i64,
    limit: Option<usize>,
) -> Result<EmbedStats, EmbedError> {
    let entities = store.all_entities()?;
    let aliases_by_id = store.aliases_grouped()?;
    let raw_facts = store.all_facts_min()?;
    let facts_by_subj: HashMap<String, Vec<(String, String)>> = raw_facts
        .into_par_iter()
        .filter_map(|(subj, pred, obj)| {
            if pred.starts_with("ref:") { return None; }
            let disp = obj_to_display(&obj)?;
            let trimmed = disp.trim().to_string();
            if trimmed.is_empty() { return None; }
            Some((subj, (pred, trimmed)))
        })
        .fold(HashMap::<String, Vec<(String, String)>>::new, |mut acc, (subj, pair)| {
            acc.entry(subj).or_default().push(pair);
            acc
        })
        .reduce(HashMap::<String, Vec<(String, String)>>::new, |mut a, b| {
            for (k, mut v) in b { a.entry(k).or_default().append(&mut v); }
            a
        });

    let prior = store.entity_embedding_meta()?;
    let model = embed.model().to_string();

    // Build (id, card_text, card_hash, stale) per entity in parallel.
    // Stale + limit gating runs sequentially after to keep ordering deterministic.
    let scored: Vec<(String, String, String, bool)> = entities
        .par_iter()
        .map(|(id, etype)| {
            let aliases = aliases_by_id.get(id).cloned().unwrap_or_default();
            let facts = facts_by_subj.get(id).cloned().unwrap_or_default();
            let card = build_card_text(id, etype, &aliases, &facts);
            let hash = card_hash(&card);
            let stale = match prior.get(id) {
                Some((prev_hash, prev_model)) => prev_model != &model || prev_hash != &hash,
                None => true,
            };
            (id.clone(), card, hash, stale)
        })
        .collect();

    let considered = scored.len();
    let mut skipped = 0usize;
    let mut queue: Vec<(String, String, String)> = Vec::new();
    for (id, card, hash, stale) in scored {
        if !force && !stale {
            skipped += 1;
            continue;
        }
        queue.push((id, card, hash));
        if let Some(l) = limit { if queue.len() >= l { break; } }
    }

    let mut stats = EmbedStats {
        considered,
        embedded: 0,
        skipped_unchanged: skipped,
        batches: 0,
    };

    let chunk_size = batch_size.max(1);
    for chunk in queue.chunks(chunk_size) {
        let texts: Vec<String> = chunk.iter().map(|(_, c, _)| c.clone()).collect();
        let vecs = embed.embed_batch(&texts).await?;
        if vecs.len() != chunk.len() {
            return Err(EmbedError::Llm(LlmError::Api(format!(
                "embed_batch returned {} vectors for {} inputs",
                vecs.len(), chunk.len()
            ))));
        }
        let rows: Vec<(String, Vec<f32>, String, String, i64)> = chunk
            .iter()
            .zip(vecs.into_iter())
            .map(|((id, _card, hash), v)| (id.clone(), v, model.clone(), hash.clone(), when))
            .collect();
        let n = store.bulk_put_entity_embeddings(&rows)?;
        stats.embedded += n;
        stats.batches += 1;
    }
    Ok(stats)
}

/// Batched section embed with cache. `sections` is `(section_hash, text)`.
/// Returns a map covering every input hash. Pre-existing entries are read
/// from the cache; the rest are embedded in chunks of `batch_size`.
pub async fn embed_sections_batch(
    store: &Store,
    embed: &dyn EmbedClient,
    sections: &[(String, String)],
    batch_size: usize,
    when: i64,
) -> Result<HashMap<String, Vec<f32>>, EmbedError> {
    let mut out: HashMap<String, Vec<f32>> = HashMap::with_capacity(sections.len());
    let mut to_embed: Vec<(String, String)> = Vec::new();
    for (hash, text) in sections {
        if let Some(v) = store.get_section_embedding(hash, embed.model())? {
            out.insert(hash.clone(), v);
        } else {
            to_embed.push((hash.clone(), text.clone()));
        }
    }
    let chunk_size = batch_size.max(1);
    for chunk in to_embed.chunks(chunk_size) {
        let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();
        let vecs = embed.embed_batch(&texts).await?;
        if vecs.len() != chunk.len() {
            return Err(EmbedError::Llm(LlmError::Api(format!(
                "embed_batch returned {} for {}", vecs.len(), chunk.len()
            ))));
        }
        for ((hash, _), v) in chunk.iter().zip(vecs.into_iter()) {
            store.put_section_embedding(hash, &v, embed.model(), when)?;
            out.insert(hash.clone(), v);
        }
    }
    Ok(out)
}

/// Embed one section text, caching by section_hash + model so repeated calls
/// don't re-embed. Used by the hybrid prefilter at runtime.
pub async fn embed_section_cached(
    store: &Store,
    embed: &dyn EmbedClient,
    section_hash: &str,
    text: &str,
    when: i64,
) -> Result<Vec<f32>, EmbedError> {
    if let Some(v) = store.get_section_embedding(section_hash, embed.model())? {
        return Ok(v);
    }
    let texts = vec![text.to_string()];
    let mut vecs = embed.embed_batch(&texts).await?;
    let v = vecs.pop().ok_or_else(|| LlmError::Api("empty embedding response".into()))?;
    store.put_section_embedding(section_hash, &v, embed.model(), when)?;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn card_text_includes_type_and_id() {
        let aliases = vec![
            ("Bob".to_string(), 0.6),
            ("Robert Tables".to_string(), 1.0),
            ("rtables".to_string(), 0.8),
        ];
        let facts = vec![
            ("title".to_string(), "Senior Engineer".to_string()),
            ("bio".to_string(), "x".repeat(500)), // long → ranked last, truncated
            ("dept".to_string(), "Platform".to_string()),
        ];
        let card = build_card_text("emp_42", "employees", &aliases, &facts);
        assert!(card.contains("employees"));
        assert!(card.contains("emp_42"));
        // Highest-confidence alias appears before lower-confidence one.
        let robert = card.find("Robert Tables").unwrap();
        let bob = card.find("Bob").unwrap();
        assert!(robert < bob);
        // Short fact "dept: Platform" makes the cut; 500-char bio gets pushed
        // out by length-based ranking (TOP_FACTS=5 but if exceeded total cap…
        // we just assert short ones are present).
        assert!(card.contains("title: Senior Engineer"));
        assert!(card.contains("dept: Platform"));
    }

    #[test]
    fn card_hash_changes_with_content() {
        let h1 = card_hash("a");
        let h2 = card_hash("b");
        let h3 = card_hash("a");
        assert_ne!(h1, h2);
        assert_eq!(h1, h3);
    }
}
