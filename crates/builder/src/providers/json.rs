//! JSON provider. Top-level array → one Entry per element.
//! Top-level object → one Entry. Anything else → one Entry (the whole doc).
//!
//! ID key inferred from the first 3 elements: pick a field that is
//! scalar + present + unique in the sample. Prefer "id"-ish names.

use super::{Entry, Provider};
use anyhow::{Context, Result};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

pub struct JsonProvider;

const SAMPLE_SIZE: usize = 3;

impl Provider for JsonProvider {
    fn name(&self) -> &'static str {
        "json"
    }

    fn matches(&self, path: &Path) -> bool {
        matches!(
            path.extension().and_then(|s| s.to_str()),
            Some("json") | Some("jsonl")
        )
    }

    fn fetch(&self, path: &Path) -> Result<Vec<Entry>> {
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

        let items: Vec<Value> = if ext == "jsonl" {
            parse_jsonl(&bytes)?
        } else {
            let v: Value = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse json {}", path.display()))?;
            match v {
                Value::Array(items) => items,
                other => vec![other],
            }
        };

        let id_key = infer_id_key(&items);

        Ok(items
            .into_iter()
            .enumerate()
            .map(|(i, item)| value_to_entry(i, item, id_key.as_deref()))
            .collect())
    }
}

fn parse_jsonl(bytes: &[u8]) -> Result<Vec<Value>> {
    let text = std::str::from_utf8(bytes).context("jsonl not utf8")?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line)
            .with_context(|| format!("jsonl line {}", i + 1))?;
        out.push(v);
    }
    Ok(out)
}

fn value_to_entry(idx: usize, v: Value, id_key: Option<&str>) -> Entry {
    let mut meta = BTreeMap::new();
    let id = id_key
        .and_then(|k| v.as_object()?.get(k))
        .and_then(scalar_string)
        .map(|s| slug(&s))
        .unwrap_or_else(|| format!("{}", idx));

    if let Value::Object(map) = &v {
        for (k, val) in map {
            meta.insert(k.clone(), value_scalar(val));
        }
    }

    let text = serde_json::to_string_pretty(&v).unwrap_or_default();
    Entry {
        id,
        kind: "json.object".into(),
        text,
        meta,
    }
}

/// Pick the best field to use as id by sampling the first N objects.
fn infer_id_key(items: &[Value]) -> Option<String> {
    let sample: Vec<&Map<String, Value>> = items
        .iter()
        .take(SAMPLE_SIZE)
        .filter_map(|v| v.as_object())
        .collect();
    if sample.is_empty() {
        return None;
    }

    let mut best: Option<(String, i32)> = None;
    for key in sample[0].keys() {
        let vals: Vec<String> = sample
            .iter()
            .filter_map(|m| m.get(key).and_then(scalar_string))
            .collect();
        if vals.len() != sample.len() {
            continue;
        }
        let uniq: HashSet<&String> = vals.iter().collect();
        if uniq.len() != vals.len() {
            continue;
        }
        let score = score_key_name(key);
        if best.as_ref().map(|(_, s)| score > *s).unwrap_or(true) {
            best = Some((key.clone(), score));
        }
    }
    best.map(|(k, _)| k)
}

fn score_key_name(k: &str) -> i32 {
    let lk = k.to_ascii_lowercase();
    if lk == "id" || lk == "uuid" {
        100
    } else if lk.ends_with("_id") {
        90
    } else if lk.contains("id") {
        50
    } else if lk == "name" || lk.ends_with("_name") {
        30
    } else {
        10
    }
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn value_scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}
