//! Process-wide chat counters. Cheap atomics; updated by every chat impl
//! after a successful response. Read via `snapshot()` to compute deltas
//! around a workload (e.g. one ingest run).

use std::sync::atomic::{AtomicU64, Ordering};

static CHAT_CALLS:    AtomicU64 = AtomicU64::new(0);
static CHAT_IN_TOK:   AtomicU64 = AtomicU64::new(0);
static CHAT_OUT_TOK:  AtomicU64 = AtomicU64::new(0);
static CHAT_THINK_TOK:AtomicU64 = AtomicU64::new(0);
static CHAT_TOTAL_MS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct ChatStats {
    pub calls: u64,
    pub in_tokens: u64,
    pub out_tokens: u64,
    pub thinking_tokens: u64,
    pub total_ms: u64,
}

impl ChatStats {
    pub fn delta(self, prev: ChatStats) -> ChatStats {
        ChatStats {
            calls: self.calls.saturating_sub(prev.calls),
            in_tokens: self.in_tokens.saturating_sub(prev.in_tokens),
            out_tokens: self.out_tokens.saturating_sub(prev.out_tokens),
            thinking_tokens: self.thinking_tokens.saturating_sub(prev.thinking_tokens),
            total_ms: self.total_ms.saturating_sub(prev.total_ms),
        }
    }
}

pub fn snapshot() -> ChatStats {
    ChatStats {
        calls: CHAT_CALLS.load(Ordering::Relaxed),
        in_tokens: CHAT_IN_TOK.load(Ordering::Relaxed),
        out_tokens: CHAT_OUT_TOK.load(Ordering::Relaxed),
        thinking_tokens: CHAT_THINK_TOK.load(Ordering::Relaxed),
        total_ms: CHAT_TOTAL_MS.load(Ordering::Relaxed),
    }
}

pub(crate) fn record(in_tok: u32, out_tok: u32, think_tok: u32, ms: u64) {
    CHAT_CALLS.fetch_add(1, Ordering::Relaxed);
    CHAT_IN_TOK.fetch_add(in_tok as u64, Ordering::Relaxed);
    CHAT_OUT_TOK.fetch_add(out_tok as u64, Ordering::Relaxed);
    CHAT_THINK_TOK.fetch_add(think_tok as u64, Ordering::Relaxed);
    CHAT_TOTAL_MS.fetch_add(ms, Ordering::Relaxed);
}
