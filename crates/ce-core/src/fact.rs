use serde::{Deserialize, Serialize};

use crate::document::{SourceRef, Value};
use crate::entity::EntityId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub source: SourceRef,
    pub adapter: String,
    pub confidence: f32,
    pub observed_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fact {
    pub subject: EntityId,
    pub predicate: String,
    pub object: Value,
    pub provenance: Provenance,
}
