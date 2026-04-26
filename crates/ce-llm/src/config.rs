use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::LlmError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    LmStudio,
    OpenAi,
    OpenRouter,
    Gemini,
    Anthropic,
}

impl ProviderKind {
    pub fn default_base_url(&self) -> &'static str {
        match self {
            ProviderKind::LmStudio => "http://127.0.0.1:1234/v1",
            ProviderKind::OpenAi => "https://api.openai.com/v1",
            ProviderKind::OpenRouter => "https://openrouter.ai/api/v1",
            ProviderKind::Gemini => "https://generativelanguage.googleapis.com/v1beta",
            ProviderKind::Anthropic => "https://api.anthropic.com/v1",
        }
    }

    /// True if the provider speaks the OpenAI chat-completions wire format.
    pub fn is_openai_compatible(&self) -> bool {
        matches!(
            self,
            ProviderKind::LmStudio | ProviderKind::OpenAi | ProviderKind::OpenRouter
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LlmConfig {
    pub provider: ProviderKind,
    pub model: String,
    #[serde(default)]
    pub embed_model: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_concurrency() -> usize { 4 }
// 8 retries × full-jitter exponential (cap 60s) survives multi-minute quota
// cooldowns without orphaning sections. Per-section work is small (<2s) so
// retrying is cheap; giving up means losing that section's LLM facts.
fn default_max_retries() -> u32 { 8 }
fn default_timeout_secs() -> u64 { 30 }

#[derive(Deserialize)]
struct CeToml { llm: LlmConfig }

impl LlmConfig {
    pub fn effective_base_url(&self) -> String {
        self.base_url.clone().unwrap_or_else(|| self.provider.default_base_url().to_string())
    }

    pub fn effective_api_key(&self) -> Result<String, LlmError> {
        if let Some(k) = self.api_key.as_ref().filter(|s| !s.is_empty()) {
            return Ok(k.clone());
        }
        if let Ok(k) = std::env::var("CE_LLM_API_KEY") {
            if !k.is_empty() { return Ok(k); }
        }
        // Provider-specific fallbacks for convenience.
        let fallback_var = match self.provider {
            ProviderKind::OpenAi => Some("OPENAI_API_KEY"),
            ProviderKind::OpenRouter => Some("OPENROUTER_API_KEY"),
            ProviderKind::Gemini => Some("GEMINI_API_KEY"),
            ProviderKind::Anthropic => Some("ANTHROPIC_API_KEY"),
            ProviderKind::LmStudio => None,
        };
        if let Some(v) = fallback_var {
            if let Ok(k) = std::env::var(v) {
                if !k.is_empty() { return Ok(k); }
            }
        }
        // LM Studio ignores the key but rig/openai client requires non-empty.
        if matches!(self.provider, ProviderKind::LmStudio) {
            return Ok("lm-studio".into());
        }
        Err(LlmError::Config(format!(
            "no api key for provider {:?}; set CE_LLM_API_KEY or {}",
            self.provider,
            fallback_var.unwrap_or("api_key in ce.toml"),
        )))
    }

    pub fn from_toml_str(s: &str) -> Result<Self, LlmError> {
        let parsed: CeToml = toml::from_str(s).map_err(|e| LlmError::Config(e.to_string()))?;
        Ok(parsed.llm)
    }

    pub fn load(path: Option<&Path>) -> Result<Self, LlmError> {
        let candidate = match path {
            Some(p) => Some(p.to_path_buf()),
            None => {
                let default = std::path::PathBuf::from("ce.toml");
                if default.exists() { Some(default) } else { None }
            }
        };
        if let Some(p) = candidate {
            let body = std::fs::read_to_string(&p)
                .map_err(|e| LlmError::Config(format!("read {}: {}", p.display(), e)))?;
            return Self::from_toml_str(&body);
        }
        // Fall back to env-only config.
        Self::from_env()
    }

    pub fn from_env() -> Result<Self, LlmError> {
        let provider = match std::env::var("CE_LLM_PROVIDER").ok().as_deref() {
            Some("lmstudio") | Some("lm-studio") => ProviderKind::LmStudio,
            Some("openai") => ProviderKind::OpenAi,
            Some("openrouter") => ProviderKind::OpenRouter,
            Some("gemini") => ProviderKind::Gemini,
            Some("anthropic") => ProviderKind::Anthropic,
            Some(other) => return Err(LlmError::Config(format!("unknown provider {}", other))),
            None => ProviderKind::OpenRouter,
        };
        let model = std::env::var("CE_LLM_MODEL")
            .unwrap_or_else(|_| match provider {
                ProviderKind::OpenRouter => "anthropic/claude-haiku-4.5".into(),
                ProviderKind::OpenAi => "gpt-4o-mini".into(),
                ProviderKind::Gemini => "gemini-2.0-flash-exp".into(),
                ProviderKind::Anthropic => "claude-haiku-4-5-20251001".into(),
                ProviderKind::LmStudio => "google/gemma-4-26b-a4b".into(),
            });
        let embed_model = std::env::var("CE_LLM_EMBED_MODEL").ok();
        let base_url = std::env::var("CE_LLM_BASE_URL").ok();
        Ok(Self {
            provider,
            model,
            embed_model,
            base_url,
            api_key: None,
            concurrency: default_concurrency(),
            max_retries: default_max_retries(),
            timeout_secs: default_timeout_secs(),
        })
    }
}
