use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub type Value = serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub path: PathBuf,
    pub byte_range: Option<Range<usize>>,
    pub locator: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSection {
    pub heading: Option<String>,
    pub text: String,
    pub locator: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentRef {
    pub path: PathBuf,
    pub mime_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Document {
    Records {
        schema_hint: Option<String>,
        records: Vec<Record>,
        source: SourceRef,
    },
    Text {
        title: Option<String>,
        sections: Vec<TextSection>,
        attachments: Vec<DocumentRef>,
        source: SourceRef,
    },
    Mixed {
        fields: HashMap<String, Value>,
        body: TextSection,
        source: SourceRef,
    },
}
