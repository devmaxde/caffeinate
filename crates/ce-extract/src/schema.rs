use std::collections::{HashMap, HashSet};

use ce_core::{Document, Entity, Fact, Provenance, Record, SourceRef};
use serde_json::Value;

pub type IdIndex = HashMap<String, HashSet<String>>;

pub struct DiscoveredDoc<'a> {
    pub schema: String,
    pub id_column: Option<String>,
    pub records: &'a [Record],
    pub source: &'a SourceRef,
}

pub fn discover_id_column(records: &[Record], schema_hint: &str) -> Option<String> {
    if records.is_empty() { return None; }
    let first = &records[0];
    let mut candidates: Vec<&String> = first.fields.keys().collect();

    let singular = schema_hint.trim_end_matches('s');
    let preferred = [
        "id".to_string(),
        format!("{}_id", schema_hint),
        format!("{}_id", singular),
        format!("{}Id", schema_hint),
        format!("{}Id", singular),
    ];

    for p in &preferred {
        if candidates.iter().any(|k| k.eq_ignore_ascii_case(p)) {
            let chosen = candidates.iter().find(|k| k.eq_ignore_ascii_case(p)).unwrap();
            if column_unique(records, chosen) {
                return Some((*chosen).clone());
            }
        }
    }

    candidates.retain(|k| {
        let lk = k.to_ascii_lowercase();
        lk == "id" || lk.ends_with("_id") || lk.ends_with("id")
    });
    for c in candidates {
        if column_unique(records, c) { return Some(c.clone()); }
    }
    None
}

fn column_unique(records: &[Record], col: &str) -> bool {
    let mut seen = HashSet::new();
    for r in records {
        let Some(v) = r.fields.get(col) else { return false; };
        let key = value_to_string(v);
        if key.is_empty() { return false; }
        if !seen.insert(key) { return false; }
    }
    true
}

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn build_id_index(docs: &[DiscoveredDoc<'_>]) -> IdIndex {
    let mut idx: IdIndex = HashMap::new();
    for d in docs {
        let Some(col) = &d.id_column else { continue; };
        let set = idx.entry(d.schema.clone()).or_default();
        for r in d.records {
            if let Some(v) = r.fields.get(col) {
                let s = value_to_string(v);
                if !s.is_empty() { set.insert(s); }
            }
        }
    }
    idx
}

pub fn detect_fks(records: &[Record], own_schema: &str, idx: &IdIndex) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if records.is_empty() { return out; }
    let cols: Vec<String> = records[0].fields.keys().cloned().collect();
    for col in cols {
        let sample: Vec<String> = records.iter()
            .filter_map(|r| r.fields.get(&col).map(value_to_string))
            .filter(|s| !s.is_empty())
            .take(20)
            .collect();
        if sample.is_empty() { continue; }
        let mut best: Option<(String, usize)> = None;
        for (schema, ids) in idx {
            if schema == own_schema { continue; }
            let hits = sample.iter().filter(|s| ids.contains(*s)).count();
            if hits * 2 >= sample.len() && hits > 0 {
                if best.as_ref().map(|(_, h)| hits > *h).unwrap_or(true) {
                    best = Some((schema.clone(), hits));
                }
            }
        }
        if let Some((s, _)) = best { out.insert(col, s); }
    }
    out
}

pub fn emit_facts(
    doc: &DiscoveredDoc<'_>,
    fks: &HashMap<String, String>,
    adapter: &str,
    observed_at: i64,
) -> (Vec<Entity>, Vec<Fact>) {
    let mut entities = Vec::new();
    let mut facts = Vec::new();
    let Some(id_col) = &doc.id_column else { return (entities, facts); };

    for r in doc.records {
        let Some(id_val) = r.fields.get(id_col) else { continue; };
        let id = value_to_string(id_val);
        if id.is_empty() { continue; }

        entities.push(Entity { id: id.clone(), entity_type: doc.schema.clone() });

        for (k, v) in &r.fields {
            if k == id_col { continue; }
            let provenance = Provenance {
                source: doc.source.clone(),
                adapter: adapter.to_string(),
                confidence: 1.0,
                observed_at,
            };
            let predicate = if let Some(target) = fks.get(k) {
                format!("ref:{}", target)
            } else {
                k.clone()
            };
            facts.push(Fact { subject: id.clone(), predicate, object: v.clone(), provenance });
        }
    }
    (entities, facts)
}

/// Identify columns whose values are predominantly long free-form text.
/// Generic: works on any record-shaped document. A column qualifies if at least
/// `min_ratio` of its non-empty string values exceed `min_chars`.
pub fn detect_text_fields(records: &[Record], min_chars: usize, min_ratio: f32) -> Vec<String> {
    if records.is_empty() { return Vec::new(); }
    let mut col_long: HashMap<String, (usize, usize)> = HashMap::new(); // col -> (long, total_str)
    for r in records {
        for (k, v) in &r.fields {
            if let Value::String(s) = v {
                let trimmed = s.trim();
                if trimmed.is_empty() { continue; }
                let entry = col_long.entry(k.clone()).or_insert((0, 0));
                entry.1 += 1;
                if trimmed.chars().count() >= min_chars { entry.0 += 1; }
            }
        }
    }
    let mut out: Vec<String> = col_long
        .into_iter()
        .filter(|(_, (long, total))| *total > 0 && (*long as f32 / *total as f32) >= min_ratio)
        .map(|(k, _)| k)
        .collect();
    out.sort();
    out
}

/// A unit of unstructured text discovered inside a structured record.
/// Carries enough provenance to attribute extracted facts back to the parent entity.
#[derive(Debug, Clone)]
pub struct TextJob {
    pub parent_subject: String,
    pub parent_type: String,
    pub field: String,
    pub text: String,
    pub source: SourceRef,
}

/// Yield extraction jobs for every long-text field in every record of a discovered document.
/// Generic: no schema-specific knowledge. Parent subject = entity id from the discovered id_column.
pub fn text_jobs_from_doc(doc: &DiscoveredDoc<'_>, min_chars: usize, min_ratio: f32) -> Vec<TextJob> {
    let mut out = Vec::new();
    let Some(id_col) = &doc.id_column else { return out; };
    let fields = detect_text_fields(doc.records, min_chars, min_ratio);
    if fields.is_empty() { return out; }
    for r in doc.records {
        let Some(id_val) = r.fields.get(id_col) else { continue; };
        let id = value_to_string(id_val);
        if id.is_empty() { continue; }
        for f in &fields {
            let Some(Value::String(s)) = r.fields.get(f) else { continue; };
            let trimmed = s.trim();
            if trimmed.chars().count() < min_chars { continue; }
            let mut src = doc.source.clone();
            src.locator = Some(format!("{}={}/{}", id_col, id, f));
            out.push(TextJob {
                parent_subject: id.clone(),
                parent_type: doc.schema.clone(),
                field: f.clone(),
                text: trimmed.to_string(),
                source: src,
            });
        }
    }
    out
}

pub fn discover_doc<'a>(doc: &'a Document) -> Option<DiscoveredDoc<'a>> {
    match doc {
        Document::Records { schema_hint, records, source } => {
            let schema = schema_hint.clone().unwrap_or_else(|| "unknown".into());
            let id_column = discover_id_column(records, &schema);
            Some(DiscoveredDoc { schema, id_column, records, source })
        }
        _ => None,
    }
}
