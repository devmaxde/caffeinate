use std::path::Path;

use async_trait::async_trait;
use ce_core::{Adapter, AdapterError, Document, Match, SourceRef, TextSection};

pub struct PdfAdapter;

fn ext_is(path: &Path, e: &str) -> bool {
    path.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case(e)).unwrap_or(false)
}

#[async_trait]
impl Adapter for PdfAdapter {
    fn name(&self) -> &'static str { "pdf" }

    fn matches(&self, path: &Path, sniff: &[u8]) -> Match {
        if ext_is(path, "pdf") { return Match::Strong; }
        if sniff.starts_with(b"%PDF-") { return Match::Strong; }
        Match::No
    }

    async fn read(&self, path: &Path) -> Result<Vec<Document>, AdapterError> {
        let pb = path.to_path_buf();
        let text = tokio::task::spawn_blocking(move || pdf_extract::extract_text(&pb))
            .await
            .map_err(|e| AdapterError::Other(e.to_string()))?
            .map_err(|e| AdapterError::Parse(e.to_string()))?;

        let title = path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string());
        let source = SourceRef { path: path.to_path_buf(), byte_range: None, locator: None };

        let sections: Vec<TextSection> = text
            .split('\u{000C}')
            .enumerate()
            .filter(|(_, s)| !s.trim().is_empty())
            .map(|(i, s)| TextSection {
                heading: None,
                text: s.to_string(),
                locator: Some(format!("page:{}", i + 1)),
            })
            .collect();

        let sections = if sections.is_empty() {
            vec![TextSection { heading: None, text, locator: None }]
        } else {
            sections
        };

        Ok(vec![Document::Text { title, sections, attachments: vec![], source }])
    }
}
