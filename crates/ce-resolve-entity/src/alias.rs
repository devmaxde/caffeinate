use std::collections::HashMap;

use ce_store::{normalize_alias, AliasRow, Store, StoreError};
use rayon::prelude::*;
use serde_json::Value;

#[derive(Debug, Default)]
pub struct AliasStats {
    pub entities: usize,
    pub aliases_written: usize,
    pub fields_used: Vec<(String, String)>, // (entity_type, predicate)
}

/// Walk all entities + their string-valued facts and produce an alias bundle
/// per entity. Bulk-inserts into `entity_aliases`.
///
/// Generic detection rule for which predicates contribute aliases:
///   - per (entity_type, predicate) compute uniqueness ratio + median length over
///     non-empty string objects.
///   - keep predicates with uniqueness >= 0.8 AND median length in [3, 60]
///     AND not flagged as long-text (median length < 200).
///
/// This catches `name`, `Name`, `full_name`, `email`, `username`, etc. in ANY
/// schema without hardcoding. Long fields like email body are excluded by the
/// length cap.
pub fn derive_aliases_from_store(store: &mut Store) -> Result<AliasStats, StoreError> {
    let entities = store.all_entities()?; // Vec<(id, entity_type)>
    let mut by_id_type: HashMap<String, String> = HashMap::with_capacity(entities.len());
    for (id, t) in &entities {
        by_id_type.insert(id.clone(), t.clone());
    }
    let blocklist = store.all_resolution_blocks()?;

    // (subject, predicate, object_json)
    let all_facts = store.all_facts_min()?;

    // Pass 1: compute alias-field set per entity_type.
    // Map (etype, predicate) -> Stats { unique values, lengths }
    let counters: HashMap<(String, String), FieldCounter> = all_facts
        .par_iter()
        .filter_map(|(subj, pred, obj_json)| {
            if pred.starts_with("ref:") { return None; }
            let etype = by_id_type.get(subj)?;
            let s = string_from_obj(obj_json)?;
            let trimmed = s.trim();
            if trimmed.is_empty() { return None; }
            Some(((etype.clone(), pred.clone()), trimmed.to_string()))
        })
        .fold(HashMap::<(String, String), FieldCounter>::new, |mut acc, (key, val)| {
            let c = acc.entry(key).or_default();
            c.total += 1;
            c.length_sum += val.chars().count();
            c.unique.insert(val);
            acc
        })
        .reduce(HashMap::<(String, String), FieldCounter>::new, |mut a, b| {
            for (k, v) in b {
                a.entry(k).or_default().merge(v);
            }
            a
        });

    // Foreign-key guard: a predicate whose values mostly equal existing entity
    // ids is pointing AT other entities, not describing the subject. Aliasing
    // its values back onto the subject (e.g. customer.purchased = "B0B3MMYHYW"
    // → alias customer-as-product-id) corrupts the index. Generic, no hard-
    // coded predicate names.
    let entity_ids: std::collections::HashSet<&str> =
        entities.iter().map(|(id, _)| id.as_str()).collect();

    let mut alias_predicates: std::collections::HashSet<(String, String)> = Default::default();
    let mut fields_used: Vec<(String, String)> = Vec::new();
    for ((etype, pred), c) in &counters {
        if c.total < 2 { continue; }
        let uniq_ratio = c.unique.len() as f32 / c.total as f32;
        let avg_len = c.length_sum as f32 / c.total as f32;
        if !(uniq_ratio >= 0.8 && (3.0..=60.0).contains(&avg_len)) { continue; }
        // Skip if >30% of values look like foreign-key references.
        let fk_hits = c.unique.iter()
            .filter(|v| entity_ids.contains(v.as_str()))
            .count();
        let fk_ratio = fk_hits as f32 / c.unique.len().max(1) as f32;
        if fk_ratio > 0.3 { continue; }
        alias_predicates.insert((etype.clone(), pred.clone()));
        fields_used.push((etype.clone(), pred.clone()));
    }
    fields_used.sort();

    // Pass 2: emit AliasRows.
    let mut rows: Vec<AliasRow> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = Default::default();
    let push = |rows: &mut Vec<AliasRow>, seen: &mut std::collections::HashSet<(String, String)>,
                    entity_id: &str, alias: String, source: String, conf: f64| {
        let norm = normalize_alias(&alias);
        if norm.len() < 2 { return; }
        // Source 'id' is always allowed; for everything else require the value
        // to look name-shaped (skip dates, currency, phone, paths).
        if source != "id" && !looks_like_alias_value(&norm) { return; }
        // Honour human rejections (Phase H.3): a (alias_norm, entity_id) pair
        // marked as blocked must never be re-derived.
        if blocklist.contains(&(norm.clone(), entity_id.to_string())) { return; }
        if !seen.insert((entity_id.to_string(), norm.clone())) { return; }
        rows.push(AliasRow {
            entity_id: entity_id.to_string(),
            alias,
            alias_norm: norm,
            source,
            confidence: conf,
        });
    };

    // 2a: id-derived aliases. Use the id verbatim — never a substring.
    // A bare numeric tail ("1002" of "emp_1002", "10" of a UUID ending in
    // "...8b10") is ambiguous: it collides with sentiment_ids, integers in
    // prose, and the tails of unrelated UUIDs. The full id is already what
    // a real reference looks like in source text.
    for (id, _t) in &entities {
        push(&mut rows, &mut seen, id, id.clone(), "id".into(), 1.0);
    }

    // 2b: alias-shaped predicates. Parallel candidate production; sequential
    // dedup maintains "id-source wins" ordering established by Pass 2a.
    let candidates: Vec<(String, String, String, f64)> = all_facts
        .par_iter()
        .filter_map(|(subj, pred, obj_json)| {
            let etype = by_id_type.get(subj)?;
            if !alias_predicates.contains(&(etype.clone(), pred.clone())) { return None; }
            let s = string_from_obj(obj_json)?;
            let trimmed = s.trim();
            if trimmed.is_empty() { return None; }
            let mut emitted: Vec<(String, String, String, f64)> = Vec::with_capacity(3);
            let source = format!("field:{}", pred);
            emitted.push((subj.clone(), trimmed.to_string(), source, 1.0));
            if let Some(local) = email_local(trimmed) {
                if let Some(name) = humanize_email_local(&local) {
                    emitted.push((subj.clone(), name, "email_local".into(), 0.7));
                }
                emitted.push((subj.clone(), local, "email_local".into(), 0.9));
            }
            Some(emitted)
        })
        .flatten()
        .collect();

    for (subj, alias, source, conf) in candidates {
        push(&mut rows, &mut seen, &subj, alias, source, conf);
    }

    let n = store.bulk_upsert_aliases(&rows)?;
    Ok(AliasStats {
        entities: entities.len(),
        aliases_written: n,
        fields_used,
    })
}

#[derive(Default)]
struct FieldCounter {
    total: usize,
    unique: std::collections::HashSet<String>,
    length_sum: usize,
}

impl FieldCounter {
    fn merge(&mut self, other: FieldCounter) {
        self.total += other.total;
        self.length_sum += other.length_sum;
        self.unique.extend(other.unique);
    }
}

fn string_from_obj(obj_json: &str) -> Option<String> {
    let v: Value = serde_json::from_str(obj_json).ok()?;
    match v {
        Value::String(s) => Some(s),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// True if a normalized alias string is plausibly a name/email/handle and not
/// a date, currency, phone, path, or pure number. Generic rules:
///   - must contain at least one ASCII letter
///   - must not contain `/` (paths/URLs)
///   - must not parse as a date-shape (YYYY-MM-DD, MM/DD/YYYY)
///   - must not be predominantly digits (>70% digit chars)
fn looks_like_alias_value(norm: &str) -> bool {
    if !norm.chars().any(|c| c.is_ascii_alphabetic()) { return false; }
    if norm.contains('/') { return false; }
    let digit_count = norm.chars().filter(|c| c.is_ascii_digit()).count();
    let total = norm.chars().count();
    if total > 0 && (digit_count as f32 / total as f32) > 0.7 { return false; }
    // crude date-shape: 8+ digits with two separators
    let seps = norm.chars().filter(|c| *c == '-' || *c == ':').count();
    if digit_count >= 6 && seps >= 2 { return false; }
    true
}


fn email_local(s: &str) -> Option<String> {
    let at = s.find('@')?;
    if at == 0 { return None; }
    let local = &s[..at];
    if !local.contains('.') && !local.contains('_') && !local.contains('-') && local.len() < 4 {
        return None;
    }
    Some(local.to_string())
}

fn humanize_email_local(local: &str) -> Option<String> {
    // "ravi.kumar" -> "Ravi Kumar". Skip if it's just an opaque alphanumeric.
    if !(local.contains('.') || local.contains('_') || local.contains('-')) { return None; }
    let parts: Vec<String> = local
        .split(|c: char| c == '.' || c == '_' || c == '-')
        .filter(|p| !p.is_empty() && p.chars().any(|c| c.is_alphabetic()))
        .map(|p| {
            let mut cs = p.chars();
            match cs.next() {
                Some(f) => f.to_uppercase().chain(cs.flat_map(|c| c.to_lowercase())).collect(),
                None => String::new(),
            }
        })
        .collect();
    if parts.len() < 2 { return None; }
    Some(parts.join(" "))
}
