use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::{LlmConfig, ProviderKind};
use crate::error::LlmError;

#[async_trait]
pub trait EmbedClient: Send + Sync {
    fn model(&self) -> &str;
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError>;
}

pub fn build_embed_client(cfg: &LlmConfig) -> Result<Arc<dyn EmbedClient>, LlmError> {
    let model = cfg.embed_model.clone().ok_or_else(|| {
        LlmError::Config("embed_model not set in ce.toml / CE_LLM_EMBED_MODEL".into())
    })?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()?;
    Ok(match cfg.provider {
        ProviderKind::LmStudio | ProviderKind::OpenAi | ProviderKind::OpenRouter => {
            Arc::new(OpenAiEmbed {
                http,
                base_url: cfg.effective_base_url(),
                api_key: cfg.effective_api_key()?,
                model,
                max_retries: cfg.max_retries,
            })
        }
        ProviderKind::Gemini => Arc::new(GeminiEmbed {
            http,
            base_url: cfg.effective_base_url(),
            api_key: cfg.effective_api_key()?,
            model,
            max_retries: cfg.max_retries,
        }),
        ProviderKind::Anthropic => {
            return Err(LlmError::Unsupported(
                "Anthropic has no embeddings endpoint; use a different embed provider".into(),
            ));
        }
    })
}

// ---- OpenAI-compatible ----

struct OpenAiEmbed {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    max_retries: u32,
}

#[derive(Serialize)]
struct OaiEmbReq<'a> { model: &'a str, input: &'a [String] }

#[derive(Deserialize)]
struct OaiEmbResp {
    #[serde(default)]
    data: Vec<OaiEmbItem>,
    #[serde(default)]
    error: Option<OaiEmbErr>,
}

#[derive(Deserialize)]
struct OaiEmbErr { message: String }

#[derive(Deserialize)]
struct OaiEmbItem { embedding: Vec<f32> }

#[async_trait]
impl EmbedClient for OpenAiEmbed {
    fn model(&self) -> &str { &self.model }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        if texts.is_empty() { return Ok(vec![]); }
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));
        let req = OaiEmbReq { model: &self.model, input: texts };
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let resp = self.http.post(&url).bearer_auth(&self.api_key).json(&req).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if status.as_u16() == 429 || status.is_server_error() {
                if attempt < self.max_retries {
                    let backoff = Duration::from_millis(250 * (1 << attempt.min(5)));
                    warn!(%status, attempt, ?backoff, "retrying embed");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }
            if !status.is_success() {
                return Err(LlmError::Api(format!("{}: {}", status, text)));
            }
            let parsed: OaiEmbResp = serde_json::from_str(&text)
                .map_err(|e| LlmError::Parse(e.to_string()))?;
            if let Some(e) = parsed.error { return Err(LlmError::Api(e.message)); }
            return Ok(parsed.data.into_iter().map(|i| i.embedding).collect());
        }
    }
}

// ---- Gemini (per-text endpoint; we batch sequentially) ----

struct GeminiEmbed {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    max_retries: u32,
}

#[derive(Serialize)]
struct GemEmbReq<'a> {
    model: String,
    content: GemEmbContent<'a>,
}

#[derive(Serialize)]
struct GemEmbContent<'a> { parts: Vec<GemEmbPart<'a>> }

#[derive(Serialize)]
struct GemEmbPart<'a> { text: &'a str }

#[derive(Deserialize)]
struct GemEmbResp {
    #[serde(default)]
    embedding: Option<GemEmbVal>,
    #[serde(default)]
    error: Option<GemEmbErr>,
}

#[derive(Deserialize)]
struct GemEmbErr { message: String }

#[derive(Deserialize)]
struct GemEmbVal { values: Vec<f32> }

#[async_trait]
impl EmbedClient for GeminiEmbed {
    fn model(&self) -> &str { &self.model }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        let mut out = Vec::with_capacity(texts.len());
        for t in texts {
            let url = format!(
                "{}/models/{}:embedContent?key={}",
                self.base_url.trim_end_matches('/'), self.model, self.api_key,
            );
            let req = GemEmbReq {
                model: format!("models/{}", self.model),
                content: GemEmbContent { parts: vec![GemEmbPart { text: t }] },
            };
            let mut attempt = 0u32;
            let v = loop {
                attempt += 1;
                let resp = self.http.post(&url).json(&req).send().await?;
                let status = resp.status();
                let body = resp.text().await?;
                if status.as_u16() == 429 || status.is_server_error() {
                    if attempt < self.max_retries {
                        let backoff = Duration::from_millis(250 * (1 << attempt.min(5)));
                        warn!(%status, attempt, ?backoff, "retrying gemini embed");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                }
                if !status.is_success() {
                    return Err(LlmError::Api(format!("{}: {}", status, body)));
                }
                let parsed: GemEmbResp = serde_json::from_str(&body)
                    .map_err(|e| LlmError::Parse(e.to_string()))?;
                if let Some(e) = parsed.error { return Err(LlmError::Api(e.message)); }
                break parsed.embedding
                    .ok_or_else(|| LlmError::Api("no embedding in response".into()))?
                    .values;
            };
            out.push(v);
        }
        Ok(out)
    }
}
