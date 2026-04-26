//! Phase B — alias derivation. Reads from the store and emits alias rows
//! covering id-variants, name-shaped fields, and email locals. Generic; no
//! schema-specific names.

pub mod alias;
pub mod embed;
pub mod prefilter;
pub mod reconcile;

pub use alias::{derive_aliases_from_store, AliasStats};
pub use embed::{
    build_card_text, card_hash, embed_section_cached, embed_sections_batch,
    run_entity_embed_pass, EmbedError, EmbedStats,
};
pub use prefilter::{
    build_alias_index, prefilter_hybrid, prefilter_with_aliases, topk_cosine, AliasHit,
};
pub use reconcile::{reconcile_cross_schema_refs, ReconcileStats};
