use std::path::Path;

use async_trait::async_trait;
use thiserror::Error;

use crate::document::Document;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Match {
    Strong,
    Weak,
    No,
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(String),
    #[error("other: {0}")]
    Other(String),
}

#[async_trait]
pub trait Adapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn matches(&self, path: &Path, sniff: &[u8]) -> Match;
    async fn read(&self, path: &Path) -> Result<Vec<Document>, AdapterError>;
}
