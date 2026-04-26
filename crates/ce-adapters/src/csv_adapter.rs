use std::path::Path;

use async_trait::async_trait;
use ce_core::{Adapter, AdapterError, Document, Match, Record, SourceRef};

pub struct CsvAdapter;

fn ext_is(path: &Path, e: &str) -> bool {
    path.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case(e)).unwrap_or(false)
}

#[async_trait]
impl Adapter for CsvAdapter {
    fn name(&self) -> &'static str { "csv" }

    fn matches(&self, path: &Path, _sniff: &[u8]) -> Match {
        if ext_is(path, "csv") || ext_is(path, "tsv") { Match::Strong } else { Match::No }
    }

    async fn read(&self, path: &Path) -> Result<Vec<Document>, AdapterError> {
        let path = path.to_path_buf();
        let docs = tokio::task::spawn_blocking(move || -> Result<Vec<Document>, AdapterError> {
            let delim = if ext_is(&path, "tsv") { b'\t' } else { b',' };
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(delim)
                .has_headers(true)
                .from_path(&path)
                .map_err(|e| AdapterError::Parse(e.to_string()))?;
            let headers = rdr.headers().map_err(|e| AdapterError::Parse(e.to_string()))?.clone();
            let mut records = Vec::new();
            for row in rdr.records() {
                let row = row.map_err(|e| AdapterError::Parse(e.to_string()))?;
                let mut fields = std::collections::HashMap::new();
                for (h, v) in headers.iter().zip(row.iter()) {
                    fields.insert(h.to_string(), serde_json::Value::String(v.to_string()));
                }
                records.push(Record { fields });
            }
            let schema_hint = path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string());
            let source = SourceRef { path: path.clone(), byte_range: None, locator: None };
            Ok(vec![Document::Records { schema_hint, records, source }])
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))??;
        Ok(docs)
    }
}
