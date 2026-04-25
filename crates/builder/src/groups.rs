//! Entity groups. One per provider-matched file.
//!
//! A group = "every record this file contributed". Carries the union of
//! every key the entries used (schema), plus *all* entries (downstream
//! consumers may need the whole set, not a sample).

use crate::providers::Entry;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct EntityGroup {
    /// Friendly label, file stem. e.g. "customers".
    pub name: String,
    /// Unique key = logical path. e.g. "/Customer_Relation_Management/customers.json".
    pub source_path: String,
    /// Provider name: "json" | "csv".
    pub provider: String,
    /// Union of all `meta` keys across entries.
    pub keys: BTreeSet<String>,
    /// Every record the file produced.
    pub entries: Vec<Entry>,
}

impl EntityGroup {
    pub fn new(name: String, source_path: String, provider: String, entries: Vec<Entry>) -> Self {
        let mut keys = BTreeSet::new();
        for e in &entries {
            for k in e.meta.keys() {
                keys.insert(k.clone());
            }
        }
        Self {
            name,
            source_path,
            provider,
            keys,
            entries,
        }
    }
}
