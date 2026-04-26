use std::collections::HashMap;

use ce_store::{ConflictRow, FactMeta, Store, StoreError};
use tracing::debug;

pub mod cardinality;

pub struct ResolverConfig {
    pub source_priority: HashMap<String, i32>,
    pub min_confidence: f64,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        let mut p = HashMap::new();
        p.insert("csv".into(), 100);
        p.insert("json".into(), 100);
        p.insert("pdf".into(), 50);
        p.insert("txt".into(), 50);
        p.insert("llm".into(), 20);
        Self { source_priority: p, min_confidence: 0.7 }
    }
}

pub enum Decision {
    PreferA,
    PreferB,
    Inbox,
}

pub fn decide(a: &FactMeta, b: &FactMeta, cfg: &ResolverConfig) -> Decision {
    let pa = *cfg.source_priority.get(&a.adapter).unwrap_or(&0);
    let pb = *cfg.source_priority.get(&b.adapter).unwrap_or(&0);
    if pa != pb {
        return if pa > pb { Decision::PreferA } else { Decision::PreferB };
    }
    let ca_ok = a.confidence >= cfg.min_confidence;
    let cb_ok = b.confidence >= cfg.min_confidence;
    if ca_ok != cb_ok {
        return if ca_ok { Decision::PreferA } else { Decision::PreferB };
    }
    if a.observed_at != b.observed_at {
        return if a.observed_at > b.observed_at { Decision::PreferA } else { Decision::PreferB };
    }
    Decision::Inbox
}

pub fn run(store: &Store, cfg: &ResolverConfig, now: i64) -> Result<ResolveStats, StoreError> {
    let mut stats = ResolveStats::default();
    let conflicts: Vec<ConflictRow> = store.unresolved_conflicts()?;
    for c in conflicts {
        let a = store.fact_meta(c.fact_a_id)?;
        let b = store.fact_meta(c.fact_b_id)?;
        match decide(&a, &b, cfg) {
            Decision::PreferA => {
                store.resolve_conflict(c.id, &format!("prefer:{}", a.id), now)?;
                stats.resolved += 1;
            }
            Decision::PreferB => {
                store.resolve_conflict(c.id, &format!("prefer:{}", b.id), now)?;
                stats.resolved += 1;
            }
            Decision::Inbox => {
                debug!(conflict_id=c.id, "left for inbox");
                stats.inbox += 1;
            }
        }
    }
    Ok(stats)
}

#[derive(Default, Debug)]
pub struct ResolveStats {
    pub resolved: usize,
    pub inbox: usize,
}
