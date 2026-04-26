//! Phase F — cross-schema reference reconciliation.
//!
//! After the alias index is built, scan every string-valued fact whose
//! predicate is NOT already a `ref:*` and look the value up in the alias
//! index. If it points unambiguously to an entity of a different type,
//! emit a `ref:<that_type>` fact. Generic — no schema-specific predicate or
//! type names involved.

use std::collections::{HashMap, HashSet};

use ce_core::{Fact, Provenance, SourceRef};
use ce_store::{normalize_alias, Store, StoreError};
use rayon::prelude::*;
use serde_json::Value;

use crate::prefilter::build_alias_index;

/// Scan a long free-form text for unambiguous, high-confidence alias mentions.
/// Used for body / description fields where the value isn't itself an id but
/// may *contain* references to other entities (e.g. an email body that names
/// a client business). Generic — drives entirely off the alias index.
fn mention_hits(
    text: &str,
    alias_index: &HashMap<String, Vec<(String, String, f64)>>,
    min_alias_conf: f64,
    min_norm_len: usize,
) -> Vec<(String, String, f64, String)> {
    let tokens: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric() && c != '\'' && c != '-')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut out: Vec<(String, String, f64, String)> = Vec::new();
    for n in (1..=5).rev() {
        if tokens.len() < n { continue; }
        for w in tokens.windows(n) {
            let candidate = w.join(" ");
            let norm = normalize_alias(&candidate);
            if norm.chars().count() < min_norm_len { continue; }
            let Some(hits) = alias_index.get(&norm) else { continue; };
            // Only mention-emit when globally unambiguous — one entity owns
            // this alias_norm. Ambiguous mentions are LLM-resolver territory.
            if hits.len() != 1 { continue; }
            let (eid, etype, conf) = &hits[0];
            if *conf < min_alias_conf { continue; }
            if seen.insert((etype.clone(), eid.clone())) {
                out.push((eid.clone(), etype.clone(), *conf, candidate));
            }
        }
    }
    out
}

#[derive(Debug, Default)]
pub struct ReconcileStats {
    pub facts_scanned: usize,
    pub refs_emitted: usize,
    pub ambiguous_skipped: usize,
    /// Refs emitted via mention-scan in long values (subset of `refs_emitted`).
    pub mention_refs: usize,
}

/// Min length (chars) above which a string-fact value is treated as free-form
/// text and gets mention-scanned. Below it, only whole-value match is used.
const MENTION_MIN_CHARS: usize = 60;
/// Mention-derived refs require this minimum normalized n-gram length to avoid
/// noise from short tokens like "ana" or "x" hitting common aliases.
const MENTION_MIN_NORM_LEN: usize = 5;
/// Only structural / high-confidence aliases (>= this) drive mention-derived
/// refs. Excludes LLM-derived (0.7) and email-locals fallback (0.7).
const MENTION_MIN_ALIAS_CONF: f64 = 0.9;

/// Walk facts, emit ref:<type> facts for unambiguous cross-type alias hits.
///
/// `confidence_discount` multiplies the alias confidence (default 0.9 mirrors
/// plan D.3 wording for derived-link discounting).
pub fn reconcile_cross_schema_refs(
    store: &Store,
    observed_at: i64,
    confidence_discount: f32,
) -> Result<ReconcileStats, StoreError> {
    let alias_rows = store.all_aliases_with_type()?;
    let alias_index = build_alias_index(alias_rows);

    // entity_id -> entity_type
    let entities = store.all_entities()?;
    let id_to_type: HashMap<String, String> =
        entities.into_iter().map(|(i, t)| (i, t)).collect();

    // Pre-fetch existing (subject, predicate, object_id) for ref:* facts so we
    // never insert a duplicate.
    let existing_refs = fetch_existing_refs(store)?;

    let all_facts = store.all_facts_min()?;

    #[derive(Default)]
    struct ScanAcc {
        scanned: usize,
        ambiguous: usize,
        mention_refs: usize,
        facts: Vec<Fact>,
    }

    let acc: ScanAcc = all_facts
        .par_iter()
        .fold(ScanAcc::default, |mut acc, (subject, predicate, object_json)| {
            if predicate.starts_with("ref:") { return acc; }
            let Some(subject_type) = id_to_type.get(subject) else { return acc; };
            let Some(s) = string_from_obj(object_json) else { return acc; };
            let trimmed = s.trim();
            if trimmed.is_empty() { return acc; }
            acc.scanned += 1;

            if trimmed.chars().count() >= MENTION_MIN_CHARS {
                for (target_id, target_type, conf, _surface) in mention_hits(
                    trimmed,
                    &alias_index,
                    MENTION_MIN_ALIAS_CONF,
                    MENTION_MIN_NORM_LEN,
                ) {
                    if target_id == *subject { continue; }
                    if &target_type == subject_type && target_id == *subject { continue; }
                    let ref_pred = format!("ref:{}", target_type);
                    let object_id_json = format!(
                        "\"{}\"",
                        target_id.replace('\\', "\\\\").replace('"', "\\\""),
                    );
                    if existing_refs.contains(&(
                        subject.clone(), ref_pred.clone(), object_id_json,
                    )) {
                        continue;
                    }
                    acc.facts.push(Fact {
                        subject: subject.clone(),
                        predicate: ref_pred,
                        object: Value::String(target_id),
                        provenance: Provenance {
                            source: SourceRef {
                                path: "alias-reconcile".into(),
                                byte_range: None,
                                locator: Some(format!("mention:{}", predicate)),
                            },
                            adapter: "alias-reconcile".into(),
                            confidence: (conf as f32) * confidence_discount,
                            observed_at,
                        },
                    });
                    acc.mention_refs += 1;
                }
                return acc;
            }

            let norm = normalize_alias(trimmed);
            if norm.len() < 2 { return acc; }
            let Some(hits) = alias_index.get(&norm) else { return acc; };

            let mut by_type: HashMap<&str, Vec<(&str, f64)>> = HashMap::new();
            for (eid, etype, conf) in hits {
                if eid == subject { continue; }
                by_type.entry(etype.as_str()).or_default().push((eid.as_str(), *conf));
            }
            let mut winners: Vec<(&str, &str, f64)> = Vec::new();
            for (etype, entries) in &by_type {
                if entries.len() == 1 {
                    winners.push((*etype, entries[0].0, entries[0].1));
                }
            }
            if winners.len() != 1 {
                if !winners.is_empty() || by_type.values().any(|v| v.len() > 1) {
                    acc.ambiguous += 1;
                }
                return acc;
            }
            let (target_type, target_id, conf) = winners[0];
            if target_type == subject_type && target_id == subject { return acc; }

            let ref_pred = format!("ref:{}", target_type);
            let object_id_json = format!("\"{}\"", target_id.replace('\\', "\\\\").replace('"', "\\\""));
            if existing_refs.contains(&(subject.clone(), ref_pred.clone(), object_id_json.clone())) {
                return acc;
            }

            acc.facts.push(Fact {
                subject: subject.clone(),
                predicate: ref_pred,
                object: Value::String(target_id.to_string()),
                provenance: Provenance {
                    source: SourceRef { path: "alias-reconcile".into(), byte_range: None, locator: None },
                    adapter: "alias-reconcile".into(),
                    confidence: (conf as f32) * confidence_discount,
                    observed_at,
                },
            });
            acc
        })
        .reduce(ScanAcc::default, |mut a, mut b| {
            a.scanned += b.scanned;
            a.ambiguous += b.ambiguous;
            a.mention_refs += b.mention_refs;
            a.facts.append(&mut b.facts);
            a
        });

    let mut stats = ReconcileStats {
        facts_scanned: acc.scanned,
        refs_emitted: 0,
        ambiguous_skipped: acc.ambiguous,
        mention_refs: acc.mention_refs,
    };
    let to_insert = acc.facts;

    let mut emitted_keys: HashSet<(String, String, String)> = HashSet::new();
    for f in &to_insert {
        let object_json = serde_json::to_string(&f.object)?;
        let key = (f.subject.clone(), f.predicate.clone(), object_json);
        if !emitted_keys.insert(key) { continue; }
        store.insert_fact(f, None)?;
        stats.refs_emitted += 1;
    }
    Ok(stats)
}

fn fetch_existing_refs(store: &Store) -> Result<HashSet<(String, String, String)>, StoreError> {
    Ok(store.all_ref_facts()?.into_iter().collect())
}

fn string_from_obj(obj_json: &str) -> Option<String> {
    let v: Value = serde_json::from_str(obj_json).ok()?;
    match v {
        Value::String(s) => Some(s),
        _ => None,
    }
}
