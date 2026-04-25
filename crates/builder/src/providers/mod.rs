//! Provider trait + Registry. Each provider knows one source format.
//! `fetch` parses a single file into a list of logical Entries.

use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;

pub mod csv;
pub mod json;

/// One logical record produced by a provider. Format-agnostic.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Stable id within the source file. Falls back to row index.
    pub id: String,
    /// Provider-defined kind label, e.g. "json.object", "csv.row".
    pub kind: String,
    /// Human-readable text dump (used by FUSE/API and later by LLM extract).
    pub text: String,
    /// Structured fields lifted from the row/object.
    pub meta: BTreeMap<String, String>,
}

pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn matches(&self, path: &Path) -> bool;
    fn fetch(&self, path: &Path) -> Result<Vec<Entry>>;
}

pub struct Registry {
    providers: Vec<Box<dyn Provider>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { providers: vec![] }
    }

    pub fn with_default() -> Self {
        let mut r = Self::new();
        r.register(Box::new(json::JsonProvider));
        r.register(Box::new(csv::CsvProvider));
        r
    }

    pub fn register(&mut self, p: Box<dyn Provider>) {
        self.providers.push(p);
    }

    pub fn for_path(&self, p: &Path) -> Option<&dyn Provider> {
        self.providers
            .iter()
            .find(|prov| prov.matches(p))
            .map(|b| b.as_ref())
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::with_default()
    }
}
