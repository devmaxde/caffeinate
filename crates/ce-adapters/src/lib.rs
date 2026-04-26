use std::path::Path;
use std::sync::Arc;

use ce_core::{Adapter, Match};

pub mod json;
pub mod csv_adapter;
pub mod txt;
pub mod pdf;

pub use csv_adapter::CsvAdapter;
pub use json::JsonAdapter;
pub use pdf::PdfAdapter;
pub use txt::TxtAdapter;

pub struct Registry {
    adapters: Vec<Arc<dyn Adapter>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { adapters: Vec::new() }
    }

    pub fn with_builtins() -> Self {
        let mut r = Self::new();
        r.register(Arc::new(JsonAdapter) as Arc<dyn Adapter>);
        r.register(Arc::new(CsvAdapter) as Arc<dyn Adapter>);
        r.register(Arc::new(TxtAdapter) as Arc<dyn Adapter>);
        r.register(Arc::new(PdfAdapter) as Arc<dyn Adapter>);
        r
    }

    pub fn register(&mut self, a: Arc<dyn Adapter>) {
        self.adapters.push(a);
    }

    pub fn pick(&self, path: &Path, sniff: &[u8]) -> Option<Arc<dyn Adapter>> {
        let mut best: Option<(Match, Arc<dyn Adapter>)> = None;
        for a in &self.adapters {
            let m = a.matches(path, sniff);
            if m == Match::No {
                continue;
            }
            let better = match &best {
                None => true,
                Some((Match::Weak, _)) if m == Match::Strong => true,
                _ => false,
            };
            if better {
                best = Some((m, a.clone()));
            }
        }
        best.map(|(_, a)| a)
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::with_builtins()
    }
}
