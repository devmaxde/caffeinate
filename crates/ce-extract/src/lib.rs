pub mod extract;
pub mod schema;

pub use extract::{
    build_prompt, extract_section, prefilter_candidates, LlmFact, LlmOut, LlmRef, ParentContext,
};
pub use ce_llm::{section_hash, ChatClient, LlmError};
pub use schema::{
    build_id_index, detect_fks, detect_text_fields, discover_doc, discover_id_column, emit_facts,
    text_jobs_from_doc, DiscoveredDoc, IdIndex, TextJob,
};
