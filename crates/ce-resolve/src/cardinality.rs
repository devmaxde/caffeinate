//! LLM-driven classification of (predicate, entity_type) pairs as `list`
//! (multi-valued, append) or `scalar` (single-valued, override).
//!
//! Schema-agnostic: the model decides from the predicate name plus sample
//! values seen in the corpus, not from a hardcoded name list.

use ce_llm::{ChatClient, LlmError};
use ce_store::{MultiValueEvidence, Store, StoreError};
use serde::Deserialize;
use tracing::{debug, warn};

#[derive(Debug, thiserror::Error)]
pub enum CardinalityError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("llm: {0}")]
    Llm(#[from] LlmError),
}

#[derive(Deserialize)]
struct Decision {
    cardinality: String,
    #[serde(default)]
    confidence: Option<f64>,
}

const SYSTEM: &str = "You decide whether a knowledge-graph predicate, on a given entity type, is single-valued (`scalar`) or multi-valued (`list`). \
Scalar = at most one current truth per entity (price, status, name, primary email). Multiple values for one entity = data conflict. \
List = many simultaneous truths per entity (tags, ordered_products, attended_events, phone_numbers, aliases). Multiple values = additive, not a conflict. \
You will see the predicate, the entity type, and example cases where one entity already holds several distinct values. Decide what the predicate semantically is. \
Output strict JSON: {\"cardinality\": \"scalar\" | \"list\", \"confidence\": number 0..1}. No prose. No markdown.";

fn build_user(ev: &MultiValueEvidence) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "predicate: {}\nentity_type: {}\n\nObserved cases (each line = one entity holding multiple distinct values):\n",
        ev.predicate, ev.entity_type
    ));
    for (i, vals) in ev.samples.iter().enumerate() {
        s.push_str(&format!("- entity {}: ", i + 1));
        let joined = vals
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        s.push_str(&joined);
        s.push('\n');
    }
    s.push_str("\nIs this predicate scalar (single-valued, multiple = conflict) or list (multi-valued, additive) for this entity type? Reply JSON only.");
    s
}

fn strip_fences(s: &str) -> &str {
    let t = s.trim();
    if let Some(rest) = t.strip_prefix("```json") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = t.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim();
    }
    t
}

/// Classify one pair via LLM. Returns ("scalar"|"list", confidence).
/// On parse / API failure, defaults to ("list", 0.0) — bias toward fewer
/// false-positive conflicts.
pub async fn classify_pair(
    client: &dyn ChatClient,
    ev: &MultiValueEvidence,
) -> (String, f64) {
    let user = build_user(ev);
    let raw = match client.chat_json(SYSTEM, &user).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error=%e, predicate=%ev.predicate, entity_type=%ev.entity_type,
                "cardinality llm call failed; defaulting to list");
            return ("list".into(), 0.0);
        }
    };
    let cleaned = strip_fences(&raw);
    match serde_json::from_str::<Decision>(cleaned) {
        Ok(d) => {
            let card = if d.cardinality == "scalar" { "scalar" } else { "list" };
            (card.into(), d.confidence.unwrap_or(0.5))
        }
        Err(e) => {
            warn!(error=%e, raw=%raw.chars().take(200).collect::<String>(),
                "cardinality llm parse failed; defaulting to list");
            ("list".into(), 0.0)
        }
    }
}

/// Classify every (predicate, entity_type) pair with multi-value evidence in
/// the store, persisting decisions to `predicate_cardinality`. Idempotent:
/// pairs already classified are skipped (see `multivalue_evidence`).
pub async fn classify_all(
    client: &dyn ChatClient,
    store: &Store,
    now: i64,
    max_subjects: usize,
    max_values_per_subject: usize,
) -> Result<ClassifyStats, CardinalityError> {
    let evidence = store.multivalue_evidence(max_subjects, max_values_per_subject)?;
    let mut stats = ClassifyStats::default();
    stats.total = evidence.len();
    for ev in evidence {
        let (card, conf) = classify_pair(client, &ev).await;
        debug!(
            predicate=%ev.predicate, entity_type=%ev.entity_type, %card, %conf,
            "predicate cardinality decided"
        );
        store.upsert_cardinality(&ev.predicate, &ev.entity_type, &card, conf, "llm", now)?;
        if card == "scalar" {
            stats.scalar += 1;
        } else {
            stats.list += 1;
        }
    }
    Ok(stats)
}

#[derive(Default, Debug)]
pub struct ClassifyStats {
    pub total: usize,
    pub scalar: usize,
    pub list: usize,
}
