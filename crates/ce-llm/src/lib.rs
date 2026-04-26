//! Provider-agnostic LLM client. Wraps reqwest directly rather than rig-core
//! because the rig API is volatile (plan risk #1) and every provider we target
//! is either OpenAI-compatible or has a stable bespoke endpoint.

pub mod chat;
pub mod config;
pub mod embed;
pub mod error;
pub mod stats;

pub use chat::{ChatClient, build_chat_client};
pub use config::{LlmConfig, ProviderKind};
pub use embed::{EmbedClient, build_embed_client};
pub use error::LlmError;
pub use stats::{snapshot, ChatStats};

pub fn section_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(text.as_bytes());
    hex::encode(h.finalize())
}
