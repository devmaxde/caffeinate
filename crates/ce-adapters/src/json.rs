use std::path::Path;

use async_trait::async_trait;
use ce_core::{Adapter, AdapterError, Document, Match, Record, SourceRef};

pub struct JsonAdapter;

fn ext_is(path: &Path, e: &str) -> bool {
    path.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case(e)).unwrap_or(false)
}

#[async_trait]
impl Adapter for JsonAdapter {
    fn name(&self) -> &'static str { "json" }

    fn matches(&self, path: &Path, sniff: &[u8]) -> Match {
        if ext_is(path, "json") || ext_is(path, "jsonl") || ext_is(path, "ndjson") {
            return Match::Strong;
        }
        let head = sniff.iter().find(|b| !b.is_ascii_whitespace()).copied();
        match head {
            Some(b'{') | Some(b'[') => Match::Weak,
            _ => Match::No,
        }
    }

    async fn read(&self, path: &Path) -> Result<Vec<Document>, AdapterError> {
        let bytes = tokio::fs::read(path).await?;
        let source = SourceRef { path: path.to_path_buf(), byte_range: None, locator: None };
        let schema_hint = path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string());

        let is_lines = ext_is(path, "jsonl") || ext_is(path, "ndjson");
        let records = if is_lines {
            let text = std::str::from_utf8(&bytes).map_err(|e| AdapterError::Parse(e.to_string()))?;
            let mut out = Vec::new();
            for line in text.lines() {
                let l = line.trim();
                if l.is_empty() { continue; }
                let v: serde_json::Value = serde_json::from_str(l).map_err(|e| AdapterError::Parse(e.to_string()))?;
                push_value(&mut out, v);
            }
            out
        } else {
            let v: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| AdapterError::Parse(e.to_string()))?;
            let mut out = Vec::new();
            match v {
                serde_json::Value::Array(arr) => {
                    for item in arr { push_value(&mut out, item); }
                }
                other => push_value(&mut out, other),
            }
            out
        };

        Ok(vec![Document::Records { schema_hint, records, source }])
    }
}

fn push_value(out: &mut Vec<Record>, v: serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            out.push(Record { fields: map.into_iter().collect() });
        }
        other => {
            let mut m = std::collections::HashMap::new();
            m.insert("value".to_string(), other);
            out.push(Record { fields: m });
        }
    }
}
