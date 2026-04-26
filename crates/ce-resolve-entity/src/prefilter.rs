use std::collections::{HashMap, HashSet};

use ce_store::{cosine, normalize_alias};

#[derive(Debug, Clone)]
pub struct AliasHit {
    pub entity_id: String,
    pub entity_type: String,
    pub surface: String,
    pub confidence: f64,
}

/// Lookup aliases inside a free-form text section. Generic: tokenizes 1..=3
/// gram windows over whitespace-separated tokens, normalizes, hits the alias
/// table.
///
/// `alias_index` maps `alias_norm -> Vec<(entity_id, entity_type, confidence)>`.
/// Build it once per ingest from `store.all_aliases_with_type()`.
pub fn build_alias_index(
    rows: Vec<(String, String, String, f64)>,
) -> HashMap<String, Vec<(String, String, f64)>> {
    let mut idx: HashMap<String, Vec<(String, String, f64)>> = HashMap::new();
    for (norm, eid, etype, conf) in rows {
        idx.entry(norm).or_default().push((eid, etype, conf));
    }
    idx
}

/// Top-K cosine search of `query` against pre-loaded entity embeddings.
/// `entities`: `(entity_id, entity_type, embedding)` for one model — supply
/// from `Store::entity_embeddings_for_model`.
pub fn topk_cosine(
    query: &[f32],
    entities: &[(String, String, Vec<f32>)],
    k: usize,
) -> Vec<(String, String, f32)> {
    if query.is_empty() || entities.is_empty() || k == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(f32, usize)> = entities
        .iter()
        .enumerate()
        .map(|(i, (_, _, e))| (cosine(query, e), i))
        .filter(|(s, _)| s.is_finite())
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter()
        .take(k)
        .map(|(s, i)| (entities[i].0.clone(), entities[i].1.clone(), s))
        .collect()
}

/// Hybrid prefilter: alias hits ∪ top-K embedding hits. De-duplicates by
/// `(entity_type, entity_id)`. Alias hits keep their alias confidence; pure
/// embedding hits use the cosine score as the confidence (clipped to [0,1]).
pub fn prefilter_hybrid(
    text: &str,
    alias_index: &HashMap<String, Vec<(String, String, f64)>>,
    section_embedding: Option<&[f32]>,
    entity_embeddings: &[(String, String, Vec<f32>)],
    max: usize,
    embed_topk: usize,
) -> Vec<AliasHit> {
    let mut out = prefilter_with_aliases(text, alias_index, max);
    if out.len() >= max { return out; }
    let Some(qv) = section_embedding else { return out; };

    let mut seen: HashSet<(String, String)> = out
        .iter()
        .map(|h| (h.entity_type.clone(), h.entity_id.clone()))
        .collect();

    for (eid, etype, score) in topk_cosine(qv, entity_embeddings, embed_topk) {
        if !seen.insert((etype.clone(), eid.clone())) { continue; }
        out.push(AliasHit {
            entity_id: eid,
            entity_type: etype,
            surface: format!("(semantic@{:.2})", score),
            confidence: score.clamp(0.0, 1.0) as f64,
        });
        if out.len() >= max { break; }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idx(rows: Vec<(&str, &str, &str, f64)>) -> HashMap<String, Vec<(String, String, f64)>> {
        build_alias_index(rows.into_iter().map(|(n, e, t, c)| (n.into(), e.into(), t.into(), c)).collect())
    }

    #[test]
    fn alias_prefilter_matches_multiword_name() {
        let ix = idx(vec![("ravi kumar", "emp_1", "employees", 1.0)]);
        let hits = prefilter_with_aliases("Hi Ravi Kumar, please review.", &ix, 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity_id, "emp_1");
    }

    #[test]
    fn cosine_topk_orders_by_similarity() {
        let entities = vec![
            ("a".into(), "t".into(), vec![1.0_f32, 0.0]),
            ("b".into(), "t".into(), vec![0.0_f32, 1.0]),
            ("c".into(), "t".into(), vec![1.0_f32, 1.0]),
        ];
        let q = vec![1.0_f32, 0.1];
        let top = topk_cosine(&q, &entities, 2);
        assert_eq!(top[0].0, "a"); // most aligned
        assert_eq!(top.len(), 2);
    }

    #[test]
    fn hybrid_dedups_alias_and_semantic() {
        let ix = idx(vec![("ravi kumar", "emp_1", "employees", 1.0)]);
        let entities = vec![
            ("emp_1".into(), "employees".into(), vec![1.0_f32, 0.0]),
            ("emp_2".into(), "employees".into(), vec![0.9_f32, 0.1]),
        ];
        let q = vec![1.0_f32, 0.0];
        let hits = prefilter_hybrid("Ravi Kumar visited.", &ix, Some(&q), &entities, 10, 5);
        // emp_1 only appears once even though both alias and semantic surface it.
        let n_emp1 = hits.iter().filter(|h| h.entity_id == "emp_1").count();
        assert_eq!(n_emp1, 1);
        assert!(hits.iter().any(|h| h.entity_id == "emp_2"));
    }
}

pub fn prefilter_with_aliases(
    text: &str,
    alias_index: &HashMap<String, Vec<(String, String, f64)>>,
    max: usize,
) -> Vec<AliasHit> {
    let tokens: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric() && c != '\'' && c != '-')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut out: Vec<AliasHit> = Vec::new();
    // 5 covers most multi-word business names (e.g. "Rodriguez, Figueroa and Sanchez").
    for n in (1..=5).rev() {
        if tokens.len() < n { continue; }
        for w in tokens.windows(n) {
            let candidate = w.join(" ");
            let norm = normalize_alias(&candidate);
            if norm.len() < 2 { continue; }
            if let Some(hits) = alias_index.get(&norm) {
                for (eid, etype, conf) in hits {
                    if seen.insert((etype.clone(), eid.clone())) {
                        out.push(AliasHit {
                            entity_id: eid.clone(),
                            entity_type: etype.clone(),
                            surface: candidate.clone(),
                            confidence: *conf,
                        });
                        if out.len() >= max { return out; }
                    }
                }
            }
        }
    }
    out
}
