//! CSV provider. Header row defines fields; each data row → Entry.
//! Id column inferred from first 3 data rows: scalar + non-empty + unique.
//! Falls back to row index.

use super::{Entry, Provider};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

pub struct CsvProvider;

const SAMPLE_SIZE: usize = 3;
const TEXT_CAP: usize = 32 * 1024;

impl Provider for CsvProvider {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn matches(&self, path: &Path) -> bool {
        matches!(path.extension().and_then(|s| s.to_str()), Some("csv"))
    }

    fn fetch(&self, path: &Path) -> Result<Vec<Entry>> {
        let mut rdr = ::csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_path(path)
            .with_context(|| format!("open csv {}", path.display()))?;

        let headers: Vec<String> = rdr
            .headers()
            .with_context(|| format!("headers {}", path.display()))?
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows: Vec<Vec<String>> = rdr
            .records()
            .enumerate()
            .map(|(i, r)| {
                r.map(|rec| rec.iter().map(|s| s.to_string()).collect())
                    .with_context(|| format!("row {} in {}", i, path.display()))
            })
            .collect::<Result<_>>()?;

        let id_idx = infer_id_col(&headers, &rows);

        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            let mut meta = BTreeMap::new();
            let mut text = String::new();
            for (col, val) in headers.iter().zip(row.iter()) {
                meta.insert(col.clone(), val.clone());
                if text.len() < TEXT_CAP {
                    text.push_str(col);
                    text.push_str(": ");
                    text.push_str(val);
                    text.push('\n');
                }
            }
            let id = id_idx
                .and_then(|idx| row.get(idx))
                .filter(|s| !s.is_empty())
                .map(|s| slug(s))
                .unwrap_or_else(|| format!("{}", i));

            out.push(Entry {
                id,
                kind: "csv.row".into(),
                text,
                meta,
            });
        }
        Ok(out)
    }
}

fn infer_id_col(headers: &[String], rows: &[Vec<String>]) -> Option<usize> {
    if rows.is_empty() {
        return None;
    }
    let sample: Vec<&Vec<String>> = rows.iter().take(SAMPLE_SIZE).collect();
    let mut best: Option<(usize, i32)> = None;
    for (idx, name) in headers.iter().enumerate() {
        let vals: Vec<&str> = sample
            .iter()
            .filter_map(|r| r.get(idx).map(|s| s.as_str()))
            .filter(|s| !s.is_empty())
            .collect();
        if vals.len() != sample.len() {
            continue;
        }
        let uniq: HashSet<&str> = vals.iter().copied().collect();
        if uniq.len() != vals.len() {
            continue;
        }
        let score = score_key_name(name);
        if best.as_ref().map(|(_, s)| score > *s).unwrap_or(true) {
            best = Some((idx, score));
        }
    }
    best.map(|(i, _)| i)
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

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}
