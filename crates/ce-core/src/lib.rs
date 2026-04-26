pub mod document;
pub mod entity;
pub mod fact;
pub mod adapter;

pub use adapter::{Adapter, AdapterError, Match};
pub use document::{Document, DocumentRef, Record, SourceRef, TextSection, Value};
pub use entity::{Entity, EntityId};
pub use fact::{Fact, Provenance};
