use std::path::Path;

use async_trait::async_trait;
use ce_core::{Adapter, AdapterError, Document, Match, SourceRef, TextSection};

pub struct TxtAdapter;

fn ext_is(path: &Path, e: &str) -> bool {
    path.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case(e)).unwrap_or(false)
}

#[async_trait]
impl Adapter for TxtAdapter {
    fn name(&self) -> &'static str { "txt" }

    fn matches(&self, path: &Path, _sniff: &[u8]) -> Match {
        if ext_is(path, "txt") || ext_is(path, "md") || ext_is(path, "log") {
            Match::Strong
        } else {
            Match::No
        }
    }

    async fn read(&self, path: &Path) -> Result<Vec<Document>, AdapterError> {
        let bytes = tokio::fs::read(path).await?;
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let title = path.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string());
        let source = SourceRef { path: path.to_path_buf(), byte_range: None, locator: None };
        Ok(vec![Document::Text {
            title,
            sections: vec![TextSection { heading: None, text, locator: None }],
            attachments: vec![],
            source,
        }])
    }
}
