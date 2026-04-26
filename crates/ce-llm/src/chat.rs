use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::{LlmConfig, ProviderKind};
use crate::error::LlmError;
use crate::stats;

#[async_trait]
pub trait ChatClient: Send + Sync {
    /// JSON-mode chat. Returns the raw assistant content (expected to be a JSON object).
    async fn chat_json(&self, system: &str, user: &str) -> Result<String, LlmError>;
    fn model(&self) -> &str;
}

pub fn build_chat_client(cfg: &LlmConfig) -> Result<Arc<dyn ChatClient>, LlmError> {
    // Pool sized for high-concurrency LLM passes (500+ in flight). Defaults
    // are generous on idle but stingy on connect; explicit values stop the
    // pool starving when many tasks fire at once.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(usize::MAX)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;
    Ok(match cfg.provider {
        ProviderKind::LmStudio | ProviderKind::OpenAi | ProviderKind::OpenRouter => {
            Arc::new(OpenAiChat {
                http,
                base_url: cfg.effective_base_url(),
                api_key: cfg.effective_api_key()?,
                model: cfg.model.clone(),
                provider: cfg.provider,
                max_retries: cfg.max_retries,
            })
        }
        ProviderKind::Gemini => Arc::new(GeminiChat {
            http,
            base_url: cfg.effective_base_url(),
            api_key: cfg.effective_api_key()?,
            model: cfg.model.clone(),
            max_retries: cfg.max_retries,
        }),
        ProviderKind::Anthropic => Arc::new(AnthropicChat {
            http,
            base_url: cfg.effective_base_url(),
            api_key: cfg.effective_api_key()?,
            model: cfg.model.clone(),
            max_retries: cfg.max_retries,
        }),
    })
}

// ---- Shared retry / backoff ----

/// Exponential backoff with full jitter, capped. Honors `Retry-After` (seconds
/// or HTTP-date is treated as seconds-only here) when the server provides it.
/// Independent jitter per task means 500 concurrent retriers don't bunch up
/// and re-stampede the API on every cycle.
fn compute_backoff(attempt: u32, retry_after: Option<u64>) -> Duration {
    if let Some(secs) = retry_after {
        // Cap at 60s so a buggy header doesn't hang a task forever.
        return Duration::from_secs(secs.min(60));
    }
    let base_ms: u64 = 500;
    let cap_ms: u64 = 60_000;
    // 2^attempt grows fast; clamp the exponent before shifting.
    let exp = attempt.min(10);
    let upper = base_ms.saturating_mul(1u64 << exp).min(cap_ms);
    let jitter_ms = fastrand::u64(0..=upper);
    Duration::from_millis(jitter_ms)
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers.get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

// ---- OpenAI-compatible (LM Studio, OpenAI, OpenRouter) ----

struct OpenAiChat {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    provider: ProviderKind,
    max_retries: u32,
}

#[derive(Serialize)]
struct OaiReq<'a> {
    model: &'a str,
    messages: Vec<OaiMsg<'a>>,
    /// LM Studio's OpenAI shim only accepts `json_schema` or `text` here, so
    /// we omit the field for that provider and rely on the system prompt to
    /// force JSON output.
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<OaiRespFmt>,
    temperature: f32,
}

#[derive(Serialize)]
struct OaiMsg<'a> { role: &'a str, content: &'a str }

#[derive(Serialize)]
struct OaiRespFmt { #[serde(rename = "type")] kind: &'static str }

#[derive(Deserialize)]
struct OaiResp {
    #[serde(default)]
    choices: Vec<OaiChoice>,
    #[serde(default)]
    error: Option<OaiApiError>,
    #[serde(default)]
    usage: Option<OaiUsage>,
}

#[derive(Deserialize, Default)]
struct OaiUsage {
    #[serde(default)] prompt_tokens: u32,
    #[serde(default)] completion_tokens: u32,
    #[serde(default)] total_tokens: u32,
}

#[derive(Deserialize)]
struct OaiApiError { message: String }

#[derive(Deserialize)]
struct OaiChoice { message: OaiRespMsg }

#[derive(Deserialize)]
struct OaiRespMsg { content: String }

#[async_trait]
impl ChatClient for OpenAiChat {
    fn model(&self) -> &str { &self.model }

    async fn chat_json(&self, system: &str, user: &str) -> Result<String, LlmError> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let response_format = match self.provider {
            ProviderKind::LmStudio => None,
            _ => Some(OaiRespFmt { kind: "json_object" }),
        };
        // Re-emphasise JSON-only when the provider doesn't enforce it server-side.
        let sys_full = if response_format.is_none() {
            format!("{}\nReturn ONLY a single raw JSON object. No prose, no fences.", system)
        } else {
            system.to_string()
        };
        let req = OaiReq {
            model: &self.model,
            messages: vec![
                OaiMsg { role: "system", content: &sys_full },
                OaiMsg { role: "user", content: user },
            ],
            response_format,
            temperature: 0.0,
        };
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let mut rb = self.http.post(&url).bearer_auth(&self.api_key).json(&req);
            if matches!(self.provider, ProviderKind::OpenRouter) {
                rb = rb
                    .header("HTTP-Referer", "https://github.com/local/ce")
                    .header("X-Title", "ce");
            }
            let t = std::time::Instant::now();
            debug!(provider=?self.provider, model=%self.model, %url, attempt, sys_chars=sys_full.len(), user_chars=user.len(), "POST chat");
            let resp = match rb.send().await {
                Ok(r) => r,
                Err(e) => {
                    if attempt < self.max_retries && (e.is_connect() || e.is_timeout() || e.is_request()) {
                        let backoff = compute_backoff(attempt, None);
                        warn!(error=%e, attempt, ?backoff, "retrying chat (transport)");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(e.into());
                }
            };
            let status = resp.status();
            let retry_after = parse_retry_after(resp.headers());
            let text = resp.text().await?;
            debug!(%status, attempt, ms=t.elapsed().as_millis() as u64, body_chars=text.len(), "chat response");
            if status.as_u16() == 429 || status.is_server_error() {
                if attempt < self.max_retries {
                    let backoff = compute_backoff(attempt, retry_after);
                    warn!(%status, attempt, ?backoff, retry_after_hdr=?retry_after, "retrying chat");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }
            if !status.is_success() {
                return Err(LlmError::Api(format!("{}: {}", status, text)));
            }
            let parsed: OaiResp = serde_json::from_str(&text)
                .map_err(|e| LlmError::Parse(e.to_string()))?;
            if let Some(e) = parsed.error { return Err(LlmError::Api(e.message)); }
            let usage = parsed.usage.unwrap_or_default();
            let choice = parsed.choices.into_iter().next()
                .ok_or_else(|| LlmError::Api("no choices".into()))?;
            let ms = t.elapsed().as_millis() as u64;
            let tps = if ms > 0 && usage.completion_tokens > 0 {
                (usage.completion_tokens as f64) * 1000.0 / (ms as f64)
            } else { 0.0 };
            info!(
                provider=?self.provider, model=%self.model, ms,
                in_tok=usage.prompt_tokens, out_tok=usage.completion_tokens,
                total_tok=usage.total_tokens, out_tps=format!("{:.1}", tps),
                resp_chars=choice.message.content.len(),
                "chat usage",
            );
            stats::record(usage.prompt_tokens, usage.completion_tokens, 0, ms);
            return Ok(choice.message.content);
        }
    }
}

// ---- Gemini ----

struct GeminiChat {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    max_retries: u32,
}

#[derive(Serialize)]
struct GemReq<'a> {
    contents: Vec<GemContent<'a>>,
    #[serde(rename = "systemInstruction")]
    system_instruction: GemContent<'a>,
    #[serde(rename = "generationConfig")]
    generation_config: GemGenCfg,
}

#[derive(Serialize)]
struct GemContent<'a> { parts: Vec<GemPart<'a>> }

#[derive(Serialize)]
struct GemPart<'a> { text: &'a str }

#[derive(Serialize)]
struct GemGenCfg {
    temperature: f32,
    #[serde(rename = "responseMimeType")]
    response_mime_type: &'static str,
}

#[derive(Deserialize)]
struct GemResp {
    #[serde(default)]
    candidates: Vec<GemCand>,
    #[serde(default)]
    error: Option<GemErr>,
    #[serde(default, rename = "usageMetadata")]
    usage_metadata: Option<GemUsage>,
    #[serde(default, rename = "modelVersion")]
    model_version: Option<String>,
}

#[derive(Deserialize, Default)]
struct GemUsage {
    #[serde(default, rename = "promptTokenCount")] prompt_token_count: u32,
    #[serde(default, rename = "candidatesTokenCount")] candidates_token_count: u32,
    #[serde(default, rename = "thoughtsTokenCount")] thoughts_token_count: u32,
    #[serde(default, rename = "totalTokenCount")] total_token_count: u32,
}

#[derive(Deserialize)]
struct GemErr { message: String }

#[derive(Deserialize)]
struct GemCand { content: GemRespContent }

#[derive(Deserialize)]
struct GemRespContent { #[serde(default)] parts: Vec<GemRespPart> }

#[derive(Deserialize)]
struct GemRespPart { #[serde(default)] text: String }

#[async_trait]
impl ChatClient for GeminiChat {
    fn model(&self) -> &str { &self.model }

    async fn chat_json(&self, system: &str, user: &str) -> Result<String, LlmError> {
        let url = format!(
            "{}/models/{}:generateContent?key={}",
            self.base_url.trim_end_matches('/'), self.model, self.api_key,
        );
        let req = GemReq {
            contents: vec![GemContent { parts: vec![GemPart { text: user }] }],
            system_instruction: GemContent { parts: vec![GemPart { text: system }] },
            generation_config: GemGenCfg {
                temperature: 0.0,
                response_mime_type: "application/json",
            },
        };
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let t = std::time::Instant::now();
            debug!(model=%self.model, %url, attempt, sys_chars=system.len(), user_chars=user.len(), "POST gemini");
            let send_res = self.http.post(&url).json(&req).send().await;
            let resp = match send_res {
                Ok(r) => r,
                Err(e) => {
                    // Network-level failure (connect/refused/reset). Retry the
                    // same way we retry 5xx — exponential + jitter — until
                    // max_retries is exhausted.
                    if attempt < self.max_retries && (e.is_connect() || e.is_timeout() || e.is_request()) {
                        let backoff = compute_backoff(attempt, None);
                        warn!(error=%e, attempt, ?backoff, "retrying gemini (transport)");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(e.into());
                }
            };
            let status = resp.status();
            let retry_after = parse_retry_after(resp.headers());
            let text = resp.text().await?;
            if status.as_u16() == 429 || status.is_server_error() {
                if attempt < self.max_retries {
                    let backoff = compute_backoff(attempt, retry_after);
                    warn!(%status, attempt, ?backoff, retry_after_hdr=?retry_after, "retrying gemini");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }
            if !status.is_success() {
                return Err(LlmError::Api(format!("{}: {}", status, text)));
            }
            let parsed: GemResp = serde_json::from_str(&text)
                .map_err(|e| LlmError::Parse(e.to_string()))?;
            if let Some(e) = parsed.error { return Err(LlmError::Api(e.message)); }
            let usage = parsed.usage_metadata.unwrap_or_default();
            let model_ver = parsed.model_version.unwrap_or_default();
            let cand = parsed.candidates.into_iter().next()
                .ok_or_else(|| LlmError::Api("no candidates".into()))?;
            let body = cand.content.parts.into_iter()
                .map(|p| p.text).collect::<Vec<_>>().join("");
            let ms = t.elapsed().as_millis() as u64;
            let billable_out = usage.candidates_token_count + usage.thoughts_token_count;
            let tps = if ms > 0 && billable_out > 0 {
                (billable_out as f64) * 1000.0 / (ms as f64)
            } else { 0.0 };
            info!(
                provider="gemini", model=%self.model, model_ver=%model_ver, ms,
                in_tok=usage.prompt_token_count,
                out_tok=usage.candidates_token_count,
                think_tok=usage.thoughts_token_count,
                total_tok=usage.total_token_count,
                out_tps=format!("{:.1}", tps),
                resp_chars=body.len(),
                "chat usage",
            );
            stats::record(
                usage.prompt_token_count,
                usage.candidates_token_count,
                usage.thoughts_token_count,
                ms,
            );
            return Ok(body);
        }
    }
}

// ---- Anthropic (no native embeddings) ----

struct AnthropicChat {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    max_retries: u32,
}

#[derive(Serialize)]
struct AntReq<'a> {
    model: &'a str,
    system: &'a str,
    messages: Vec<AntMsg<'a>>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Serialize)]
struct AntMsg<'a> { role: &'a str, content: &'a str }

#[derive(Deserialize)]
struct AntResp {
    #[serde(default)]
    content: Vec<AntBlock>,
    #[serde(default)]
    error: Option<AntErr>,
    #[serde(default)]
    usage: Option<AntUsage>,
}

#[derive(Deserialize, Default)]
struct AntUsage {
    #[serde(default)] input_tokens: u32,
    #[serde(default)] output_tokens: u32,
}

#[derive(Deserialize)]
struct AntErr { message: String }

#[derive(Deserialize)]
struct AntBlock {
    #[serde(default, rename = "type")]
    _kind: String,
    #[serde(default)]
    text: String,
}

#[async_trait]
impl ChatClient for AnthropicChat {
    fn model(&self) -> &str { &self.model }

    async fn chat_json(&self, system: &str, user: &str) -> Result<String, LlmError> {
        let url = format!("{}/messages", self.base_url.trim_end_matches('/'));
        // Anthropic has no JSON-mode flag; nudge via system prompt suffix.
        let sys_full = format!("{}\nReturn ONLY a single raw JSON object. No prose, no fences.", system);
        let req = AntReq {
            model: &self.model,
            system: &sys_full,
            messages: vec![AntMsg { role: "user", content: user }],
            max_tokens: 4096,
            temperature: 0.0,
        };
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let t = std::time::Instant::now();
            let resp = self.http.post(&url)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", "2023-06-01")
                .json(&req).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if status.as_u16() == 429 || status.is_server_error() {
                if attempt < self.max_retries {
                    let backoff = Duration::from_millis(250 * (1 << attempt.min(5)));
                    warn!(%status, attempt, ?backoff, "retrying anthropic");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }
            if !status.is_success() {
                return Err(LlmError::Api(format!("{}: {}", status, text)));
            }
            let parsed: AntResp = serde_json::from_str(&text)
                .map_err(|e| LlmError::Parse(e.to_string()))?;
            if let Some(e) = parsed.error { return Err(LlmError::Api(e.message)); }
            let usage = parsed.usage.unwrap_or_default();
            let body = parsed.content.into_iter().map(|b| b.text).collect::<Vec<_>>().join("");
            let ms = t.elapsed().as_millis() as u64;
            info!(
                provider="anthropic", model=%self.model, ms,
                in_tok=usage.input_tokens, out_tok=usage.output_tokens,
                resp_chars=body.len(),
                "chat usage",
            );
            stats::record(usage.input_tokens, usage.output_tokens, 0, ms);
            return Ok(body);
        }
    }
}
