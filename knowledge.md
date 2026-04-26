# `ce` — knowledge handoff

Snapshot for the next session. Captures what's built, why, where things live, how to validate, and what's left. Read top to bottom; everything below `Quick validation recipe` is reference.

---

## TL;DR

`ce` is a generic enterprise-data context engine: ingest a folder, build a fact store, derive aliases, embed entities, resolve text-mention references with an LLM, and serve markdown views + a small web UI. It works end-to-end against `EnterpriseBench/`. Local LLM target is **LM Studio at `http://127.0.0.1:1234`** running `google/gemma-4-26b-a4b` (chat) and `text-embedding-nomic-embed-text-v1.5` (embed). All five plan phases (A, B, C, D, F, plus H UX) are implemented and validated.

**Hard rule, project-wide: no schema-specific code.** Detect alias-shaped fields by uniqueness ratio + length, not by name lists. Detect FKs by alias-hit rate, not by `_id` suffix. Generic char classes are OK; lists like `["name", "Name", "full_name"]` are not.

---

## Crate map

```
crates/
  ce-core/          Entity, Fact, Provenance, SourceRef, Document/Record types
  ce-adapters/      Per-format ingest adapters (csv, json, jsonl, pdf, txt, md)
  ce-engine/        Folder walker that fans documents to adapters
  ce-store/         SQLite schema, CRUD, embeddings BLOB helpers, cosine, normalize_alias
  ce-extract/       LLM-prompt-driven section extraction (uses ChatClient trait)
  ce-resolve/       Existing conflict resolver (independent of entity resolver)
  ce-resolve-entity/ Phase B/C/D/F: alias derivation, prefilter, embed pass, reconcile
  ce-llm/           Provider-agnostic ChatClient + EmbedClient (LM Studio, OpenAI,
                    OpenRouter, Gemini, Anthropic) + LlmConfig loader
  ce-search/        Tantivy BM25 + petgraph entity-graph
  ce-views/         Markdown rendering with idempotent gen/manual blocks; Aliases section
  ce-api/           Axum HTTP API + static HTML for /ui/conflicts and /ui/resolutions
  ce-cli/           `ce` binary; orchestrates the full pipeline
```

The big new crates from this session: **ce-llm** (provider abstraction, replaces the bespoke OpenRouter shim) and **ce-resolve-entity** (alias / embed / hybrid prefilter / cross-schema reconcile / surface-form learning).

---

## Data model (SQLite — `ce.sqlite`)

Migration files: `crates/ce-store/migrations/0001_init.sql` (originals) + `0002_aliases_embeddings.sql` (Phases B + C + H.3). Both run on every `Store::open` via `execute_batch`. Plus a runtime `try_alter` to retro-fit `entity_embeddings.card_hash` onto stores that pre-date that column.

| Table | Purpose |
|---|---|
| `documents` | Source files. Delta-detection by `(path, content_hash)`. |
| `entities` | `(id, entity_type)`. `id` is opaque text. Type `document` is meta-owned by ce. |
| `facts` | `(subject, predicate, object_json, document_id, adapter, confidence, observed_at, locator)`. Predicates with prefix `ref:<type>` are graph edges; everything else is an attribute. |
| `section_cache` | Deduplicates LLM extraction per `section_hash` (sha256 of text). |
| `conflicts` | Detected attribute conflicts. `fact_a_id`/`fact_b_id` reference `facts(id)` — that's why we soft-delete (set `confidence=0`) instead of hard-deleting facts. |
| `entity_aliases` | `(entity_id, alias, alias_norm, source, confidence)`. `source` ∈ {`id`, `field:<predicate>`, `email_local`, `derived`}. PK is `(entity_id, alias_norm)`. |
| `entity_embeddings` | `(entity_id, embedding BLOB, model, card_hash, created_at)`. Embedding is little-endian f32. `card_hash` enables stale detection; switching `model` also triggers re-embed. |
| `section_embeddings` | `(section_hash, embedding BLOB, model, created_at)`. Cache for the LLM-pass hybrid prefilter. |
| `resolution_blocklist` | Phase H.3. `(alias_norm, entity_id)` pairs the human rejected. `derive_aliases_from_store` and the resolver honour this. |

BLOB encode/decode + cosine: `ce_store::{embedding_to_blob, embedding_from_blob, cosine}`. Normalisation: `ce_store::normalize_alias` (lowercase, ASCII-fold, whitespace-collapse, trim).

---

## The pipeline

`ce ingest <folder>` runs everything in order. Each phase is also exposed as a standalone subcommand for re-runs.

### 1. Structured pass (`ce-cli::ingest`)
- Walk folder via `ce-engine::walk` → adapters parse files into `Document` values.
- Delta detection per file via SHA-256 (`upsert_document_with_delta`). Cached files skip emit.
- For `Document::Records`, `discover_doc` finds the id column, `detect_fks` declares cross-record references, `emit_facts` produces (Entity, Fact) lists. Parallel via rayon, single-txn bulk insert.
- For `Document::Text`, we synthesise an entity `(id=path, type=document)` per source file so text-mention resolution has a parent subject to anchor `ref:<type>` edges to. **This is critical** — without it the LLM resolver can't emit refs from PDFs.
- `detect_conflicts_bulk` runs only when `total_facts > 0` this run. The self-join is N² in worst case over hundreds of thousands of facts and hangs otherwise.

### 2. Alias index (`ce-resolve-entity::derive_aliases_from_store`, auto-runs in ingest; standalone: `ce alias-index [--rebuild]`)
- Reads all entities + their string-valued facts.
- **Field eligibility (generic)**: per `(entity_type, predicate)` keep predicates with uniqueness ratio ≥ 0.8 AND avg value length 3–60 chars. Catches `name`, `Name`, `full_name`, `email`, etc. without naming any of them.
- **Value-shape filter**: `looks_like_alias_value` rejects values without ASCII letters, with `/` (paths), >70% digits (currency/phone/tax id), or 6+ digits with ≥2 separators (date-shape).
- **Alias sources emitted**: `id` (raw + numeric tail like `EMP-1002` → `1002`), `field:<predicate>`, `email_local` (local part + humanised "First Last" from `ravi.kumar`), `derived` (from D.4 surface-form learning and human approves).
- **Honors `resolution_blocklist`** — rejected (alias_norm, entity_id) pairs never re-derive.

### 3. Cross-schema reference reconciliation (Phase F, auto-runs; standalone: `ce reconcile-refs`)
- Scans every non-`ref:*` string fact, looks the value up in the alias index.
- If it points unambiguously to ONE entity of a DIFFERENT type, emits `ref:<that_type>` with `adapter='alias-reconcile'`, confidence = alias_conf × 0.9.
- Pre-fetches existing ref triples to dedupe; ambiguous hits (collisions) are counted but skipped.
- This handles "ALFKI" vs "Alfki Trading Co." style cross-schema linking from the original plan.

### 4. Embedding pass (Phase C, separate command: `ce embed [--limit N] [--batch-size 64] [--force]`)
- Per entity, builds a generic card text: `"<type> <id>\naliases: a1, a2, …\n<pred1>: <short_val>\n<pred2>: <short_val>"`. Aliases sorted by confidence desc. Facts sorted by **value length asc** so identifying short attributes float up and long bodies sink out (truncated at TOP_FACTS=5, total cap 2048 chars).
- Hashes the card; stale = `card_hash` mismatch OR `model` mismatch. Idempotent re-runs.
- Calls `EmbedClient::embed_batch` with batches of 64 default. Stores 768-dim f32 little-endian BLOB.
- `--limit N` is for fast validation (caps queue size, not entities considered).
- Throughput against local LM Studio + nomic-embed: ~165 entities/sec at batch 64.

### 5. LLM pass with hybrid prefilter (Phase D, gated on `--max-llm N`)
Path inside `ce-cli::ingest` (lines ~395–510):
1. Build the work list: `text_jobs` (PDF text sections + long string fields from records), filter `section_cached`, take first `--max-llm N`.
2. Decide `hybrid_ready = embed_model.is_some() && entity_embedding_count > 0`. If yes:
   - Build `EmbedClient`, batch-embed unique sections via `embed_sections_batch` (cached by `section_hash`).
   - Pre-load `entity_embeddings_for_model(model)` into `Arc<Vec<...>>` (~33k × 768 × 4 = ~100 MB).
3. Spawn `tokio::Semaphore`-gated tasks at `cfg.concurrency`. Each task:
   - Runs `prefilter_hybrid`: 1–3-gram alias hits ∪ top-30 cosine-cosine matches against entity embeddings, deduped by `(type, id)`, capped at 200 candidates.
   - Calls `extract_section(client, text, candidates, source, …, parent)` which builds the prompt, hits `ChatClient::chat_json`, parses `LlmOut { references, facts }`.
   - Converts each `LlmRef` into a `Fact` `subject=parent.subject, predicate=ref:<type from candidate map>, object=ref.entity_id, adapter='llm-resolve', confidence=ref.confidence × 0.9`.
   - **D.4 surface-form learning**: when `ref.confidence ≥ 0.8` AND `evidence_span` is present, inserts the span as a `derived` AliasRow at confidence 0.7 — next ingest's prefilter catches the same surface without an LLM call.
4. Main loop awaits handles, persists facts + aliases under one `llm` document_id per section.

Validated end-to-end: `shipping_order_prini.pdf` produced 41 facts, 14 `ref:products` edges, 4 derived aliases (3 ASIN→full-title mappings + 1 customer-id mapping) in a single 58-second gemma 26B call.

---

## CLI surface (`crates/ce-cli/src/main.rs`)

| Command | What it does |
|---|---|
| `ce ingest <folder> [--max-llm N] [--llm-concurrency M] [--llm-config PATH] [--llm-model M]` | Full pipeline. `--max-llm 0` (default) runs structured + alias + reconcile only. |
| `ce alias-index [--rebuild]` | Re-derive aliases standalone. `--rebuild` clears first. |
| `ce reconcile-refs [--confidence-discount 0.9]` | Cross-schema ref reconciliation standalone. |
| `ce embed [--llm-config] [--batch-size 64] [--force] [--limit N]` | Run / refresh entity embeddings against the configured `embed_model`. |
| `ce llm-test [--prompt …]` | One-shot round-trip to verify the configured provider works (Phase G.3). |
| `ce inspect conflicts \| resolutions [--since TS] [--limit N]` | Read-only audit. Resolutions show subject → predicate → target with quoted evidence span. |
| `ce resolve` | Existing single-entity conflict resolver (independent of entity resolution). |
| `ce build [--out out/] [--templates DIR]` | Render markdown views (idempotent gen/manual blocks). |
| `ce index [--idx ce.idx]` / `ce search QUERY [--hops N]` | BM25 + graph hops. |
| `ce serve [--addr 127.0.0.1:3000] [--idx]` | Axum HTTP API + UI. |
| `ce list <type> [--contains S] [--limit N]`, `ce show <type> <id> [--format md\|json\|facts]` | Browse the store. |
| `ce agg entity-counts \| predicate-counts \| top-values` | Aggregations. |

Notable invariant: `ce show ... --format md` now fetches per-id aliases via `Store::aliases_for_subject` and renders the new `## Aliases` template section.

---

## HTTP API + UI

`crates/ce-api/src/lib.rs` + `crates/ce-api/static/*.html`. Serve with `ce serve`.

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Index page linking to inboxes |
| `/health` | GET | "ok" |
| `/entities/:type/:id` | GET | Facts + rendered markdown JSON |
| `/retrieve` | POST | BM25 + optional graph hops; needs `--idx` |
| `/conflicts`, `/conflicts/:id`, `/conflicts/:id/resolve` | GET/POST | Conflict inbox backend |
| `/resolutions?since=&limit=&max_conf=` | GET | LLM-resolve refs filtered by confidence ceiling. Excludes already-suppressed (conf=0) entries. |
| `/resolutions/approve` | POST | Promotes the surface→entity pair to a `derived` alias at confidence 1.0 |
| `/resolutions/reject` | POST | Adds `(alias_norm, entity_id)` to `resolution_blocklist` AND sets the offending fact's confidence to 0 (soft delete — hard delete violates `conflicts.fact_a_id` FK) |
| `/ui/conflicts` | GET | Static HTML page |
| `/ui/resolutions` | GET | Static HTML page (Phase H.3) |

Approve / reject body shape:
```json
{"subject": "...", "predicate": "ref:products", "object_json": "\"B07232M876\"",
 "target_id": "B07232M876", "surface": "B07232M876 Amazonbasics …"}
```

The UI's reload control re-fetches with the user-set `max_conf` and `limit`. Approve/reject are optimistic (greys the row on success, alerts on failure).

---

## Configuration

`ce.toml` at repo root is auto-loaded from CWD. Override with `--llm-config <path>` on any subcommand that talks to LLMs. Env-var fallbacks: `CE_LLM_PROVIDER`, `CE_LLM_MODEL`, `CE_LLM_EMBED_MODEL`, `CE_LLM_BASE_URL`, `CE_LLM_API_KEY` (plus per-provider `OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY`).

Current `ce.toml`:
```toml
[llm]
provider = "lmstudio"
base_url = "http://127.0.0.1:1234/v1"
model = "google/gemma-4-26b-a4b"
embed_model = "text-embedding-nomic-embed-text-v1.5"
api_key = "lm-studio"   # LM Studio ignores the value
concurrency = 4
max_retries = 3
timeout_secs = 300       # gemma 26B locally needs minutes for long sections
```

Provider switch is one line — `provider = "gemini"` or `"openrouter"` etc. The `ChatClient`/`EmbedClient` traits give one impl per family: OpenAI-compat (LM Studio + OpenAI + OpenRouter share `OpenAiChat`), Gemini (`GeminiChat`/`GeminiEmbed`), Anthropic (`AnthropicChat`, no embed).

---

## Local LLM gotchas

- **LM Studio's OpenAI shim rejects `response_format.type="json_object"`** ("must be json_schema or text"). `OpenAiChat::chat_json` already special-cases `ProviderKind::LmStudio` and omits the field, re-emphasising "Return ONLY a single raw JSON object" in the system prompt. Don't reintroduce the field for that provider.
- **gemma 26B latency**: ~30–70 s per resolver call on local hardware. `timeout_secs=300` gives margin. `--llm-concurrency 2` keeps GPU saturation reasonable.
- **Embed throughput**: nomic-embed at batch 64 → ~165/sec. Full 33k entities runs in ~3 min.
- LM Studio's `/v1/models` lists what's loaded — useful pre-flight: `curl -s http://127.0.0.1:1234/v1/models | jq '.data[].id'`.

---

## Plan phase status

| Phase | Status | Notes |
|---|---|---|
| A — Provider abstraction (rig) | ✓ | Skipped `rig-core` (volatile API at v0.35; plan referenced 0.6). Built `ChatClient`/`EmbedClient` directly over reqwest. Same trait surface, zero version risk. |
| B — Alias index | ✓ | Generic detection; honours blocklist. |
| B.6 — Alias-aware FK detection in structured pass | partial | Covered functionally by Phase F's reconciler running after the alias index. Not folded into `detect_fks` itself. |
| C — Embeddings + hybrid prefilter | ✓ | 33k entities × 768 dim. Hybrid auto-engages when embeddings exist for current `embed_model`. |
| D — LLM resolver | ✓ | Refs → `ref:<type>` facts on parent (incl. synthetic `document` parent for PDFs). D.4 surface-form learning live. D.3 cheap-path skip not implemented; the existing `--max-llm N` gate already bounds cost. D.5 explicit conflict-on-divergent-resolution path not added; default conflict detector picks up duplicate refs naturally. |
| E — Defaults flip | ✓ | Alias index + reconcile run automatically inside `ingest`. The LLM pass remains opt-in via `--max-llm N` for cost control. |
| F — Cross-schema reconciliation | ✓ | Single-pass alias lookup over every string fact. |
| G.1 — LM Studio | ✓ | Default config in `ce.toml`. |
| G.2 — Gemini | ✓ structurally | Code path exists (`GeminiChat`/`GeminiEmbed`); not run live this session. Switch by editing `ce.toml`. |
| G.3 — `just llm-test` recipe | ✓ as `ce llm-test` subcommand | Works against the configured provider. |
| G.4 — Cost / latency safeguards | partial | Timeouts + retries + concurrency cap done. No global token budget yet. |
| H.1 — Aliases markdown section | ✓ | Grouped by source. |
| H.2 — `ce inspect resolutions` | ✓ | `--since` + `--limit` flags. |
| H.3 — Web UI Resolutions tab | ✓ | Approve / reject round-tripped against live API. |

---

## Constraints / non-goals

- **No schema-specific code.** Statistical heuristics or value-shape regex only. The hardcoded literals that ARE allowed are ones owned by ce itself: the `ref:` predicate prefix, the `document` meta-type for synthesised entities, and adapter strings (`llm`, `llm-resolve`, `alias-reconcile`).
- **No HNSW.** Brute-force cosine over ≤100k entities is fine. Plan deferred this.
- **No Postgres.** SQLite stays single-binary for the demo.
- **No OCR.** Scanned PDFs are out of scope.

---

## Quick validation recipe

Smallest end-to-end pass on a fresh checkout:

```bash
# 1. Provider sanity — needs LM Studio reachable.
cargo run -q -p ce-cli -- llm-test --prompt 'reply with {"ack":true}'

# 2. Structured + alias + reconcile (no LLM yet).
cargo run -q -p ce-cli -- ingest EnterpriseBench

# 3. Embed everything (~3 min for 33k entities).
cargo run -q -p ce-cli -- embed --batch-size 64

# 4. LLM resolver pass on a couple of sections.
RUST_LOG=info,ce_llm=debug cargo run -q -p ce-cli -- ingest EnterpriseBench --max-llm 5 --llm-concurrency 2

# 5. Inspect what landed.
cargo run -q -p ce-cli -- inspect resolutions --limit 10

# 6. UI.
cargo run -q -p ce-cli -- serve --addr 127.0.0.1:3000
# then open http://127.0.0.1:3000/ui/resolutions
```

Verify in SQLite:
```bash
sqlite3 -header -column ce.sqlite "
SELECT 'entities', count(*) FROM entities
UNION ALL SELECT 'facts', count(*) FROM facts
UNION ALL SELECT 'aliases', count(*) FROM entity_aliases
UNION ALL SELECT 'entity_embeddings', count(*) FROM entity_embeddings
UNION ALL SELECT 'reconciled_refs', count(*) FROM facts WHERE adapter='alias-reconcile'
UNION ALL SELECT 'llm-resolve_refs', count(*) FROM facts WHERE adapter='llm-resolve' AND confidence>0
UNION ALL SELECT 'derived_aliases', count(*) FROM entity_aliases WHERE source='derived'
UNION ALL SELECT 'document entities', count(*) FROM entities WHERE entity_type='document'
UNION ALL SELECT 'blocked', count(*) FROM resolution_blocklist;"
```

---

## Where to look first when something breaks

- **Ingest hangs after "bulk insert"**: it's `detect_conflicts_bulk` doing the N² self-join. Already auto-skipped when no facts inserted, but if a real run produces a lot of new facts and stalls, optimise that query (group by subject+predicate having count>1 first).
- **LLM ref count = 0 despite refs returned**: the section's parent is `None`. PDF sections need the synthetic `document` entity to be created (see step 1 of the pipeline). Long fields inside structured records get parents from their record id.
- **Embed call rejected by LM Studio**: probably `response_format` was put back in for LM Studio. Keep it `None` for that provider.
- **`derive_aliases` count looks wrong after a re-run**: `aliases_written` reports the number of UPSERT calls, not net new rows. Run `SELECT count(*) FROM entity_aliases` to see the real count. Field-eligibility is recomputed each run, so as more LLM facts land they can shift uniqueness ratios and change which predicates qualify.
- **Hard-deleting an `llm-resolve` fact fails**: `conflicts(fact_a_id) REFERENCES facts(id)` FK. Use `suppress_resolution_fact` (sets confidence=0); list endpoints filter `confidence > 0`.

---

## Project documents

- `PLAN.md` — original plan for the broader system
- `RESOLVE_PLAN.md` — detailed plan this session implemented (Phases A–H)
- `STATE.md` — older session state, may be out of date
- `DEMO.md`, `README.md` — user-facing docs (may not reflect this session's additions)
- `ce.toml`, `ce.toml.example` — local LLM config
- `EnterpriseBench/` — test data folder (1 324 files, ~33k entities)

End of handoff.
