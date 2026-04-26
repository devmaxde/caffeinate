use serde::{Deserialize, Serialize};

pub type EntityId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub entity_type: String,
}
