use std::collections::HashMap;

use ce_core::{Fact, Provenance, SourceRef};
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use ce_llm::{ChatClient, LlmError};

#[derive(Deserialize, Debug, Clone)]
pub struct LlmRef {
    pub entity_id: String,
    #[serde(default)]
    pub evidence_span: Option<String>,
    /// LLM-self-reported confidence in [0.0, 1.0]. Defaults to 0.85 when the
    /// model omits it — slightly below 1.0 so structured-pass facts (1.0)
    /// outrank LLM-derived refs in conflict resolution.
    #[serde(default = "default_ref_confidence")]
    pub confidence: f32,
}

fn default_ref_confidence() -> f32 { 0.85 }

#[derive(Deserialize, Debug)]
pub struct LlmFact {
    pub subject: String,
    pub predicate: String,
    pub object: Value,
    #[serde(default)]
    pub evidence_span: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct LlmOut {
    #[serde(default)]
    pub references: Vec<LlmRef>,
    #[serde(default)]
    pub facts: Vec<LlmFact>,
}

pub fn prefilter_candidates<'a>(
    text: &str,
    entities_by_type: &'a HashMap<String, Vec<String>>,
    max: usize,
) -> Vec<(&'a str, &'a str)> {
    let mut out = Vec::new();
    let lower = text.to_ascii_lowercase();
    for (etype, ids) in entities_by_type {
        for id in ids {
            if id.len() < 3 { continue; }
            if lower.contains(&id.to_ascii_lowercase()) {
                out.push((etype.as_str(), id.as_str()));
                if out.len() >= max { return out; }
            }
        }
    }
    out
}

/// Optional context about the entity from which `text` was extracted.
/// When set, the prompt allows the LLM to attach facts to that subject and to mark
/// references as outgoing edges from it, even if the bare id is not literally in the text.
#[derive(Debug, Clone)]
pub struct ParentContext {
    pub subject: String,
    pub entity_type: String,
    pub field: String,
}

pub fn build_prompt(
    text: &str,
    candidates: &[(&str, &str)],
    parent: Option<&ParentContext>,
) -> (String, String) {
    let system = "You ground entity references in a single document. \
        Output a strict JSON object: { references: [...], facts: [...] }. \
        \
        Hard rules — violations will be discarded by the caller: \
        (1) For every reference, evidence_span MUST be a contiguous quote copied byte-for-byte from the document text. No paraphrasing. No inventing context. \
        (2) entity_id MUST be copied verbatim from the candidate list. Do not invent IDs. Do not pick a candidate just because it looks similar to something in the text. \
        (3) If the document does not contain text that unambiguously identifies a candidate by name / id / email / handle, OMIT that reference entirely. Silence is correct when uncertain. \
        (4) Do not guess customer / employee / product / client identities from context, position, or document order — only from quoted text. \
        (5) confidence is in [0.0, 1.0]. Use ≥0.9 only when the surface form in evidence_span is unambiguous and matches exactly one candidate. \
        \
        Output a single raw JSON object. No markdown fences.".to_string();

    let mut cand_lines = String::new();
    for (t, id) in candidates {
        cand_lines.push_str(&format!("- id={} type={}\n", id, t));
    }
    if cand_lines.is_empty() {
        cand_lines.push_str("(none)\n");
    }

    let parent_block = match parent {
        Some(p) => format!(
            "Parent entity (subject for any facts about this text):\n- id={} type={} field={}\n\n",
            p.subject, p.entity_type, p.field
        ),
        None => String::new(),
    };

    let user = format!(
        "{}Candidate entities (other ids that may be referenced):\n{}\nDocument text:\n---\n{}\n---\n\nReturn JSON: {{\"references\": [{{\"entity_id\": str, \"evidence_span\": str, \"confidence\": number}}], \"facts\": [{{\"subject\": entity_id, \"predicate\": str, \"object\": any, \"evidence_span\": str}}]}}",
        parent_block, cand_lines, truncate(text, 8000)
    );
    (system, user)
}

fn strip_fences(s: &str) -> &str {
    let t = s.trim();
    if let Some(rest) = t.strip_prefix("```json") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = t.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim();
    }
    t
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max { return s.to_string(); }
    let mut end = max;
    while !s.is_char_boundary(end) { end -= 1; }
    format!("{}…", &s[..end])
}

pub async fn extract_section(
    client: &dyn ChatClient,
    text: &str,
    candidates: &[(&str, &str)],
    source: &SourceRef,
    observed_at: i64,
    confidence: f32,
    parent: Option<&ParentContext>,
) -> Result<(Vec<Fact>, Vec<LlmRef>), LlmError> {
    let (system, user) = build_prompt(text, candidates, parent);
    let raw = client.chat_json(&system, &user).await?;
    let cleaned = strip_fences(&raw);
    let parsed: LlmOut = match serde_json::from_str(cleaned) {
        Ok(v) => v,
        Err(e) => {
            warn!(error=%e, raw=%truncate(&raw, 200), "llm json parse fail");
            return Ok((vec![], vec![]));
        }
    };

    let mut facts = Vec::new();
    for f in parsed.facts {
        let prov = Provenance {
            source: SourceRef {
                path: source.path.clone(),
                byte_range: None,
                locator: f.evidence_span.clone().or_else(|| source.locator.clone()),
            },
            adapter: "llm".into(),
            confidence,
            observed_at,
        };
        facts.push(Fact {
            subject: f.subject,
            predicate: f.predicate,
            object: f.object,
            provenance: prov,
        });
    }
    Ok((facts, parsed.references))
}
