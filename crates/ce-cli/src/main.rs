use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use ce_adapters::Registry;
use ce_core::{Document, Fact, Provenance, SourceRef};
use ce_engine::{walk, WalkOpts};
use ce_extract::{
    build_id_index, detect_fks, discover_doc, emit_facts, extract_section,
    section_hash, text_jobs_from_doc, DiscoveredDoc, ParentContext,
};
use ce_llm::{build_chat_client, build_embed_client, snapshot as llm_snapshot, LlmConfig};
use ce_resolve_entity::{
    build_alias_index, derive_aliases_from_store,
    prefilter_with_aliases, reconcile_cross_schema_refs, run_entity_embed_pass,
};
use ce_store::Store;
use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ce", about = "Context engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum AggKind {
    EntityCounts {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
    },
    PredicateCounts {
        #[arg(long)]
        r#type: Option<String>,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
    },
    TopValues {
        predicate: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
    },
}

#[derive(Subcommand)]
enum Cmd {
    Ingest {
        folder: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        /// Cap the LLM pass at N text sections. Omit to process every
        /// eligible section (per-email / per-PDF-page / per-long-text-field
        /// — one prompt each).
        #[arg(long)]
        max_llm: Option<usize>,
        /// Override model from LlmConfig.
        #[arg(long)]
        llm_model: Option<String>,
        /// Override concurrency from LlmConfig.
        #[arg(long)]
        llm_concurrency: Option<usize>,
        /// Path to ce.toml; defaults to ./ce.toml then env vars.
        #[arg(long)]
        llm_config: Option<PathBuf>,
        /// Drop every prior `llm` / `llm-resolve` fact + section cache + derived
        /// aliases before running. Use after prompt or validation changes.
        #[arg(long)]
        rebuild_llm: bool,
    },
    /// Build / refresh the alias index from existing entities + facts (Phase B).
    AliasIndex {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        /// Wipe existing aliases first (otherwise upsert).
        #[arg(long)]
        rebuild: bool,
    },
    /// Embed every entity (Phase C). Uses provider's embed_model.
    Embed {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long)]
        llm_config: Option<PathBuf>,
        #[arg(long, default_value_t = 64)]
        batch_size: usize,
        /// Re-embed even if card_hash matches.
        #[arg(long)]
        force: bool,
        /// Stop after embedding this many entities (validation runs).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Cross-schema ref reconciliation: scan string facts, emit ref:<type>
    /// for unambiguous alias hits across types (Phase F).
    ReconcileRefs {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value_t = 0.9)]
        confidence_discount: f32,
        /// Drop every prior `alias-reconcile` fact (and any conflicts that
        /// referenced them) before re-emitting. Use after alias-derivation
        /// rules change — otherwise stale refs from the old rules linger.
        #[arg(long)]
        rebuild: bool,
    },
    /// LLM-classify every (predicate, entity_type) pair with multi-value
    /// evidence as `list` (additive) or `scalar` (single-valued, conflict),
    /// then re-run conflict detection. Use after ingest if classification was
    /// skipped, or to force a re-scan with `--reclassify`.
    CleanupConflicts {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long)]
        llm_config: Option<PathBuf>,
        /// Wipe the cardinality cache and reclassify every multi-value pair.
        #[arg(long)]
        reclassify: bool,
    },
    /// Round-trip a fixed prompt against the configured provider (Phase G.3).
    LlmTest {
        #[arg(long)]
        llm_config: Option<PathBuf>,
        #[arg(long, default_value = "say hi in a json object {\"greeting\":\"...\"}.")]
        prompt: String,
    },
    Query { text: String },
    Inspect {
        what: String,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        /// Only include rows observed at or after this unix timestamp.
        #[arg(long)]
        since: Option<i64>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Resolve {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
    },
    Build {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value = "out")]
        out: PathBuf,
        #[arg(long)]
        templates: Option<PathBuf>,
    },
    Index {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value = "ce.idx")]
        idx: PathBuf,
    },
    Agg {
        #[command(subcommand)]
        kind: AggKind,
    },
    Search {
        text: String,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value = "ce.idx")]
        idx: PathBuf,
        #[arg(long, default_value_t = 10)]
        k: usize,
        #[arg(long, default_value_t = 0)]
        hops: usize,
    },
    Serve {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long)]
        idx: Option<PathBuf>,
        #[arg(long, default_value = "0.0.0.0:3000")]
        addr: String,
        /// Directory for cached `/entities/:t/:id/render.md` bodies. First hit
        /// generates and writes, subsequent hits read from disk. Pass `--no-md-cache`
        /// to disable.
        #[arg(long, default_value = "md_store")]
        md_store: PathBuf,
        #[arg(long)]
        no_md_cache: bool,
    },
    /// List entity ids of a given type.
    List {
        etype: String,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long)]
        contains: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Generate the same per-entity Markdown that `/entities/:t/:id/render.md`
    /// produces for the web UI, written to disk under `--md-store`. Already-cached
    /// entities are skipped unless `--force`. Runs in parallel with exponential
    /// backoff on transient LLM errors.
    GenMd {
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value = "md_store")]
        md_store: PathBuf,
        #[arg(long)]
        llm_config: Option<PathBuf>,
        /// Restrict to one entity_type.
        #[arg(long)]
        etype: Option<String>,
        /// Stop after this many entities (post-skip).
        #[arg(long)]
        limit: Option<usize>,
        /// Concurrent LLM calls.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        /// Max retries per entity. Backoff is 1s × 2^attempt with ±20% jitter,
        /// capped at 60s.
        #[arg(long, default_value_t = 5)]
        max_retries: usize,
        /// Re-render even if the cache file already exists.
        #[arg(long)]
        force: bool,
    },
    /// Show all facts for one entity.
    Show {
        etype: String,
        id: String,
        #[arg(long, default_value = "ce.sqlite")]
        store: PathBuf,
        #[arg(long, default_value = "facts")]
        format: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        // Explicit defaults so the AI chat-flow targets (`ce_render`, `ce_ask`)
        // are never drowned by tower_http / hyper / h2 noise. Override with
        // RUST_LOG for one-off debugging.
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,ce_render=info,ce_ask=info,tower_http=warn,hyper=warn,h2=warn,reqwest=warn")
        }))
        .with_target(true)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Ingest { folder, dry_run, store, max_llm, llm_model, llm_concurrency, llm_config, rebuild_llm } => {
            ingest(folder, dry_run, store, max_llm, llm_model, llm_concurrency, llm_config, rebuild_llm).await?;
        }
        Cmd::AliasIndex { store, rebuild } => alias_index_cmd(store, rebuild)?,
        Cmd::ReconcileRefs { store, confidence_discount, rebuild } => {
            reconcile_refs_cmd(store, confidence_discount, rebuild)?
        }
        Cmd::Embed { store, llm_config, batch_size, force, limit } => {
            embed_cmd(store, llm_config, batch_size, force, limit).await?
        }
        Cmd::CleanupConflicts { store, llm_config, reclassify } => {
            cleanup_conflicts_cmd(store, llm_config, reclassify).await?
        }
        Cmd::LlmTest { llm_config, prompt } => llm_test_cmd(llm_config, prompt).await?,
        Cmd::Query { text } => info!(%text, "query (no-op)"),
        Cmd::Inspect { what, store, since, limit } => inspect(what, store, since, limit)?,
        Cmd::Resolve { store } => resolve_cmd(store)?,
        Cmd::Build { store, out, templates } => build_cmd(store, out, templates)?,
        Cmd::Index { store, idx } => index_cmd(store, idx)?,
        Cmd::Search { text, store, idx, k, hops } => search_cmd(text, store, idx, k, hops)?,
        Cmd::Agg { kind } => agg_cmd(kind)?,
        Cmd::Serve { store, idx, addr, md_store, no_md_cache } => {
            serve_cmd(store, idx, addr, md_store, no_md_cache).await?
        }
        Cmd::List { etype, store, contains, limit } => list_cmd(etype, store, contains, limit)?,
        Cmd::Show { etype, id, store, format } => show_cmd(etype, id, store, format)?,
        Cmd::GenMd { store, md_store, llm_config, etype, limit, concurrency, max_retries, force } => {
            gen_md_cmd(store, md_store, llm_config, etype, limit, concurrency, max_retries, force).await?
        }
    }
    Ok(())
}

async fn ingest(
    folder: PathBuf,
    dry_run: bool,
    store_path: PathBuf,
    max_llm: Option<usize>,
    llm_model: Option<String>,
    llm_concurrency: Option<usize>,
    llm_config_path: Option<PathBuf>,
    rebuild_llm: bool,
) -> Result<()> {
    let registry = Arc::new(Registry::with_builtins());
    let mut rx = walk(&folder, registry, WalkOpts::default());

    let mut collected: Vec<(PathBuf, String, Vec<Document>)> = Vec::new();
    let mut errors = 0usize;
    while let Some(item) = rx.recv().await {
        match item.docs {
            Ok(docs) => collected.push((item.path, item.adapter, docs)),
            Err(e) => {
                errors += 1;
                warn!(path=%item.path.display(), adapter=%item.adapter, error=%e, "adapter failed");
            }
        }
    }

    let mut all_discovered: Vec<usize> = Vec::new();
    let mut discovered_docs: Vec<DiscoveredDoc<'_>> = Vec::new();
    for (fi, (_, _, docs)) in collected.iter().enumerate() {
        for d in docs.iter() {
            if let Some(dd) = discover_doc(d) {
                discovered_docs.push(dd);
                all_discovered.push(fi);
            }
        }
    }
    let id_index = build_id_index(&discovered_docs);

    if dry_run {
        for dd in &discovered_docs {
            info!(schema=%dd.schema, id_col=?dd.id_column, records=dd.records.len(), "discovered");
        }
        info!(files=collected.len(), structured=discovered_docs.len(), errors, "ingest dry-run done");
        return Ok(());
    }

    let mut store = Store::open(&store_path)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

    if rebuild_llm {
        let n = store.clear_llm_facts()?;
        info!(dropped = n, "cleared prior llm + llm-resolve facts (and section cache)");
    }

    // Phase A: upsert documents with delta detection. Skip files whose content_hash is
    // unchanged since last ingest (Cached); for changed files, drop stale facts (Stale)
    // before re-emitting.
    let t_doc = std::time::Instant::now();
    let mut doc_ids: Vec<i64> = Vec::with_capacity(discovered_docs.len());
    let mut skip_emit: Vec<bool> = Vec::with_capacity(discovered_docs.len());
    let (mut n_fresh, mut n_cached, mut n_stale) = (0usize, 0usize, 0usize);
    for (i, _) in discovered_docs.iter().enumerate() {
        let fi = all_discovered[i];
        let (path, adapter, _) = &collected[fi];
        let path_str = path.display().to_string();
        let content_hash = file_hash(path).unwrap_or_else(|_| "unknown".to_string());
        let (did, status) =
            store.upsert_document_with_delta(&path_str, adapter, &content_hash, now)?;
        doc_ids.push(did);
        match status {
            ce_store::DeltaStatus::Fresh => n_fresh += 1,
            ce_store::DeltaStatus::Cached => n_cached += 1,
            ce_store::DeltaStatus::Stale => n_stale += 1,
        }
        skip_emit.push(matches!(status, ce_store::DeltaStatus::Cached));
    }
    info!(elapsed=?t_doc.elapsed(), fresh=n_fresh, cached=n_cached, stale=n_stale, "documents upserted");

    // Phase B: parallel emit_facts (CPU-only, no DB).
    use rayon::prelude::*;
    let t_emit = std::time::Instant::now();
    let per_doc: Vec<(Vec<ce_core::Entity>, Vec<ce_core::Fact>, usize)> = discovered_docs
        .par_iter()
        .enumerate()
        .filter(|(i, _)| !skip_emit[*i])
        .map(|(i, dd)| {
            let fi = all_discovered[i];
            let adapter = &collected[fi].1;
            let fks = detect_fks(dd.records, &dd.schema, &id_index);
            let (es, fs) = emit_facts(dd, &fks, adapter, now);
            (es, fs, i)
        })
        .collect();
    info!(elapsed=?t_emit.elapsed(), "facts emitted (parallel)");

    // Phase C: flatten + single bulk insert.
    let t_flat = std::time::Instant::now();
    let total_facts: usize = per_doc.iter().map(|(_, f, _)| f.len()).sum();
    let total_ents: usize = per_doc.iter().map(|(e, _, _)| e.len()).sum();
    let mut all_entities: Vec<ce_core::Entity> = Vec::with_capacity(total_ents);
    let mut all_facts: Vec<(ce_core::Fact, Option<i64>)> = Vec::with_capacity(total_facts);
    for (es, fs, i) in per_doc {
        let did = doc_ids[i];
        all_entities.extend(es);
        all_facts.extend(fs.into_iter().map(|f| (f, Some(did))));
    }
    info!(elapsed=?t_flat.elapsed(), entities=total_ents, facts=total_facts, "flattened");

    let t_ins = std::time::Instant::now();
    store.bulk_ingest(&all_entities, &all_facts)?;
    info!(elapsed=?t_ins.elapsed(), "bulk insert (single txn)");

    info!(files=collected.len(), entities=total_ents, facts=total_facts, errors, "structured pass done");

    // Phase B: alias index (always-on; cheap and the LLM pass needs it).
    info!("alias index starting");
    let t_alias = std::time::Instant::now();
    let alias_stats = derive_aliases_from_store(&mut store)?;
    info!(
        elapsed=?t_alias.elapsed(),
        aliases=alias_stats.aliases_written,
        alias_fields=?alias_stats.fields_used,
        "alias index built",
    );

    // Phase F: cross-schema ref reconciliation. Cheap, alias-driven, no LLM.
    info!("ref reconciliation starting");
    let t_rec = std::time::Instant::now();
    let rec_stats = reconcile_cross_schema_refs(&store, now, 0.9)?;
    info!(
        elapsed=?t_rec.elapsed(),
        scanned=rec_stats.facts_scanned,
        emitted=rec_stats.refs_emitted,
        ambiguous=rec_stats.ambiguous_skipped,
        "ref reconciliation done",
    );

    // Treat `Some(0)` as explicit "skip LLM pass". Absent flag = process every
    // eligible section.
    if matches!(max_llm, Some(0)) {
        return Ok(());
    }

    info!("llm pass: loading config");
    let mut cfg = LlmConfig::load(llm_config_path.as_deref())
        .map_err(|e| anyhow::anyhow!("llm config: {}", e))?;
    if let Some(m) = llm_model { cfg.model = m; }
    if let Some(c) = llm_concurrency { cfg.concurrency = c; }
    let client = build_chat_client(&cfg).map_err(|e| anyhow::anyhow!("llm client: {}", e))?;
    let concurrency = cfg.concurrency;
    info!(provider=?cfg.provider, model=%cfg.model, timeout_s=cfg.timeout_secs, "llm pass starting");
    let llm_pass_t0 = std::time::Instant::now();
    let llm_stats_t0 = llm_snapshot();

    // Alias-driven prefilter (Phase B.5). Replaces id-substring scan: maps name
    // mentions, email locals, and id variants to the right entity in one pass.
    let alias_rows = store.all_aliases_with_type()?;
    // Reverse view: entity_id → known alias_norms. Used by the LLM-resolve
    // grounding check so a returned ref must have at least one of its target's
    // aliases physically present in the evidence span.
    let mut alias_by_entity_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::with_capacity(alias_rows.len());
    for (norm, eid, _etype, _conf) in &alias_rows {
        alias_by_entity_map.entry(eid.clone()).or_default().push(norm.clone());
    }
    let alias_by_entity = Arc::new(alias_by_entity_map);
    let alias_index = Arc::new(build_alias_index(alias_rows));

    // Job carries optional parent context (subject/type/field). For
    // Document::Text (PDFs / .txt / .md) we synthesise a generic `document`
    // entity per source file so the resolver has a subject to attach
    // ref:<type> edges to. Long string fields inside structured records keep
    // their own parent (the record entity).
    let mut text_jobs: Vec<(PathBuf, String, String, SourceRef, Option<ParentContext>)> = Vec::new();
    let mut doc_entities_seen: std::collections::HashSet<String> = Default::default();
    for (path, adapter, docs) in &collected {
        for d in docs {
            if let Document::Text { sections, source, .. } = d {
                let doc_id = path.display().to_string();
                if doc_entities_seen.insert(doc_id.clone()) {
                    store.upsert_entity(&ce_core::Entity {
                        id: doc_id.clone(),
                        entity_type: "document".into(),
                    })?;
                }
                for s in sections {
                    let mut src = source.clone();
                    src.locator = s.locator.clone().or(src.locator);
                    let parent = ParentContext {
                        subject: doc_id.clone(),
                        entity_type: "document".into(),
                        field: s.locator.clone().unwrap_or_else(|| "body".into()),
                    };
                    text_jobs.push((path.clone(), adapter.clone(), s.text.clone(), src, Some(parent)));
                }
            }
        }
    }
    // Generic: extract long text fields from any structured record (>=200 chars, >=50% of values).
    for (i, dd) in discovered_docs.iter().enumerate() {
        let fi = all_discovered[i];
        let (path, adapter, _) = &collected[fi];
        let jobs = text_jobs_from_doc(dd, 200, 0.5);
        for j in jobs {
            let parent = ParentContext {
                subject: j.parent_subject,
                entity_type: j.parent_type,
                field: j.field,
            };
            text_jobs.push((path.clone(), adapter.clone(), j.text, j.source, Some(parent)));
        }
    }
    info!(text_sections=text_jobs.len(), ?max_llm, "llm queue");

    // Absent flag → process every section. Some(N) → cap.
    let cap = match max_llm {
        Some(n) => n.min(text_jobs.len()),
        None => text_jobs.len(),
    };
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::new();

    // Materialize the actual work list once: skip already-cached sections, hash
    // the rest, hand off to spawn loop. Lets us pre-embed the same set in batch.
    let mut work: Vec<(PathBuf, String, String, String, SourceRef, Option<ParentContext>)> = Vec::new();
    for (path, adapter, text, source, parent) in text_jobs.into_iter().take(cap) {
        let hash = section_hash(&text);
        if store.section_cached(&hash)? { continue; }
        work.push((path, adapter, text, hash, source, parent));
    }

    info!(work=work.len(), cap, "llm work materialized");

    // Persist per-task: each section's facts + aliases + section_cache row are
    // committed the moment its LLM call returns. Interrupting the run loses
    // only the in-flight tasks (≤ concurrency); everything completed is
    // durable and the next ingest skips it via section_cache.
    let store = Arc::new(tokio::sync::Mutex::new(store));
    let progress = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress_facts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress_refs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress_aliases = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_work = work.len();
    info!(tasks=total_work, concurrency, "dispatching llm tasks");

    let pb = ProgressBar::new(total_work as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] [{wide_bar:.green/dim}] {pos}/{len} ({percent}%) · ETA {eta_precise} · {per_sec} · {msg}",
        )
        .unwrap()
        .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    let pb = Arc::new(pb);

    for (idx, (path, adapter, text, hash, source, parent)) in work.into_iter().enumerate() {
        let client = client.clone();
        let alias_index = alias_index.clone();
        let alias_by_entity = alias_by_entity.clone();
        let store = store.clone();
        let progress = progress.clone();
        let progress_facts = progress_facts.clone();
        let progress_refs = progress_refs.clone();
        let progress_errors = progress_errors.clone();
        let progress_aliases = progress_aliases.clone();
        let pb = pb.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let t_total = std::time::Instant::now();
            let t_pre = std::time::Instant::now();
            // Alias-only candidates. Semantic neighbors are too easy to
            // hallucinate-pick: model latches onto a "looks similar" entity
            // it saw in the candidate list with no textual basis. Literal
            // alias hits are the only sound grounding for ref attribution.
            let hits = prefilter_with_aliases(&text, &alias_index, 200);
            tracing::debug!(idx, prefilter_ms=t_pre.elapsed().as_millis() as u64,
                text_len=text.len(), candidates=hits.len(), "llm task: prefilter done");

            // No literal alias hits → no entities to ground references to.
            // Mark cached so we don't re-attempt on every run, then exit.
            if hits.is_empty() {
                {
                    let s = store.lock().await;
                    let _ = s.mark_section(&hash, 0, now);
                }
                progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                pb.inc(1);
                return;
            }
            let id_to_type: std::collections::HashMap<String, String> = hits
                .iter()
                .map(|h| (h.entity_id.clone(), h.entity_type.clone()))
                .collect();
            let candidates: Vec<(&str, &str)> = hits
                .iter()
                .map(|h| (h.entity_type.as_str(), h.entity_id.as_str()))
                .collect();
            let t_llm = std::time::Instant::now();
            let res = extract_section(
                &*client, &text, &candidates, &source, now, 0.7, parent.as_ref(),
            ).await;
            tracing::info!(idx, llm_ms=t_llm.elapsed().as_millis() as u64,
                ok=res.is_ok(), total_ms=t_total.elapsed().as_millis() as u64, "llm task: chat done");

            // Phase D: refs returned by the LLM → ref:<type> facts on the
            // parent entity + derived aliases (D.4) when the model is confident
            // and gives us an evidence_span.
            let (extra_facts, learned_aliases, raw_refs) = match &res {
                Ok((_facts, refs)) => {
                    let mut extra = Vec::new();
                    let mut aliases: Vec<ce_store::AliasRow> = Vec::new();
                    // Build a per-task lowercased view of source text for evidence
                    // validation. Costs one allocation per task, prevents
                    // per-ref hallucinated refs from being persisted.
                    let text_lower = text.to_ascii_lowercase();
                    if let Some(p) = parent.as_ref() {
                        for r in refs {
                            let Some(etype) = id_to_type.get(&r.entity_id) else { continue; };
                            if r.entity_id == p.subject { continue; }
                            // Grounding check #1: evidence_span MUST appear in
                            // the source text. LLMs invent quotes when uncertain.
                            let Some(span) = r.evidence_span.as_deref() else { continue; };
                            let span_trim = span.trim();
                            if span_trim.is_empty() { continue; }
                            if !text_lower.contains(&span_trim.to_ascii_lowercase()) {
                                tracing::debug!(idx, entity=%r.entity_id, span=%span_trim, "drop ref: evidence not in text");
                                continue;
                            }
                            // Grounding check #2: at least one known alias of
                            // the chosen entity must appear inside the
                            // normalized span. Stops the model picking a
                            // similar-looking candidate when nothing in the
                            // text actually identifies it.
                            let span_norm = ce_store::normalize_alias(span_trim);
                            let grounded = alias_by_entity
                                .get(&r.entity_id)
                                .map(|aliases| aliases.iter().any(|a| span_norm.contains(a)))
                                .unwrap_or(false);
                            if !grounded {
                                tracing::debug!(idx, entity=%r.entity_id, span=%span_trim,
                                    "drop ref: span doesn't contain any alias of target");
                                continue;
                            }
                            let raw_conf = r.confidence.clamp(0.0, 1.0);
                            // Auto-approve: ≥0.9 LLM confidence skips the inbox
                            // entirely (full conf, alias promoted at 1.0).
                            let auto_approve = raw_conf >= 0.9;
                            let conf = if auto_approve { raw_conf } else { raw_conf * 0.9 };
                            extra.push(Fact {
                                subject: p.subject.clone(),
                                predicate: format!("ref:{}", etype),
                                object: serde_json::Value::String(r.entity_id.clone()),
                                provenance: Provenance {
                                    source: SourceRef {
                                        path: source.path.clone(),
                                        byte_range: None,
                                        locator: r.evidence_span.clone().or_else(|| source.locator.clone()),
                                    },
                                    adapter: "llm-resolve".into(),
                                    confidence: conf,
                                    observed_at: now,
                                },
                            });
                            // D.4 surface-form learning: high-confidence resolution
                            // with a quoted span becomes a derived alias.
                            // Auto-approved (≥0.9) → confidence 1.0, same as a
                            // human-clicked approve. Mid-confidence → 0.7, still
                            // visible in the resolutions inbox.
                            if r.confidence >= 0.8 {
                                if let Some(span) = r.evidence_span.as_ref() {
                                    let trimmed = span.trim();
                                    if !trimmed.is_empty() && trimmed.chars().count() <= 60 {
                                        let norm = ce_store::normalize_alias(trimmed);
                                        if norm.len() >= 2 && !norm.eq_ignore_ascii_case(&r.entity_id) {
                                            aliases.push(ce_store::AliasRow {
                                                entity_id: r.entity_id.clone(),
                                                alias: trimmed.to_string(),
                                                alias_norm: norm,
                                                source: "derived".into(),
                                                confidence: if auto_approve { 1.0 } else { 0.7 },
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    (extra, aliases, refs.len())
                }
                Err(_) => (Vec::new(), Vec::new(), 0),
            };

            // Persist NOW — before this task returns. If the user Ctrl+Cs
            // mid-pass, this section's work is already durable; only
            // currently-running tasks lose their LLM result.
            match res {
                Ok((facts, _refs)) => {
                    let path_str = path.display().to_string();
                    let extra_n = extra_facts.len();
                    let alias_n;
                    {
                        let mut s = store.lock().await;
                        let doc_id = match s.insert_document(&path_str, "llm", &hash, now) {
                            Ok(id) => id,
                            Err(e) => {
                                warn!(path=%path_str, error=%e, "store: insert_document failed");
                                progress_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }
                        };
                        for f in &facts { let _ = s.insert_fact(f, Some(doc_id)); }
                        for f in &extra_facts { let _ = s.insert_fact(f, Some(doc_id)); }
                        alias_n = if learned_aliases.is_empty() {
                            0
                        } else {
                            s.bulk_upsert_aliases(&learned_aliases).unwrap_or(0)
                        };
                        let _ = s.mark_section(&hash, facts.len() + extra_n, now);
                    }
                    progress_facts.fetch_add(facts.len(), std::sync::atomic::Ordering::Relaxed);
                    progress_refs.fetch_add(extra_n, std::sync::atomic::Ordering::Relaxed);
                    progress_aliases.fetch_add(alias_n, std::sync::atomic::Ordering::Relaxed);
                    let _ = adapter; // adapter is used for documents elsewhere; ignore here.
                    tracing::debug!(path=%path_str, facts=facts.len(), refs=raw_refs, ref_facts=extra_n, "llm persisted");
                }
                Err(e) => {
                    warn!(path=%path.display(), error=%e, "llm error");
                    progress_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let f = progress_facts.load(std::sync::atomic::Ordering::Relaxed);
            let r = progress_refs.load(std::sync::atomic::Ordering::Relaxed);
            let er = progress_errors.load(std::sync::atomic::Ordering::Relaxed);
            pb.set_message(format!("facts={} refs={} err={}", f, r, er));
            pb.inc(1);
        }));
    }

    // Drain handles. Each task already persisted its own result — this loop
    // only waits for completion and surfaces any panic.
    for h in handles {
        if let Err(e) = h.await {
            warn!(error=%e, "task join failed");
        }
    }
    pb.finish_with_message("done");
    let llm_facts = progress_facts.load(std::sync::atomic::Ordering::Relaxed);
    let llm_ref_facts = progress_refs.load(std::sync::atomic::Ordering::Relaxed);
    let llm_derived_aliases = progress_aliases.load(std::sync::atomic::Ordering::Relaxed);
    let llm_errors = progress_errors.load(std::sync::atomic::Ordering::Relaxed);

    let elapsed = llm_pass_t0.elapsed();
    let stats = llm_snapshot().delta(llm_stats_t0);
    let avg_ms = if stats.calls > 0 { stats.total_ms / stats.calls } else { 0 };
    let in_per_call  = if stats.calls > 0 { stats.in_tokens  / stats.calls } else { 0 };
    let out_per_call = if stats.calls > 0 { stats.out_tokens / stats.calls } else { 0 };
    let wall_secs = elapsed.as_secs_f64().max(1e-6);
    let in_tps  = (stats.in_tokens  as f64) / wall_secs;
    let out_tps = (stats.out_tokens as f64) / wall_secs;
    info!(
        llm_facts, llm_ref_facts, llm_derived_aliases, llm_errors,
        wall_s=format!("{:.1}", wall_secs),
        calls=stats.calls,
        in_tok=stats.in_tokens, out_tok=stats.out_tokens, think_tok=stats.thinking_tokens,
        in_per_call, out_per_call,
        avg_call_ms=avg_ms,
        in_tps=format!("{:.0}", in_tps), out_tps=format!("{:.0}", out_tps),
        "llm pass done",
    );

    // Phase G: predicate cardinality classification + conflict detection.
    // For every (entity_type, predicate) pair where some subject holds ≥2
    // distinct values, ask the LLM whether that predicate is `list` (additive,
    // no conflict) or `scalar` (single-valued, conflict). Cache the verdict.
    // Then run the bulk conflict detector, which only flags `scalar` pairs.
    {
        let s = store.lock().await;
        info!("cardinality classification starting");
        let t_card = std::time::Instant::now();
        match ce_resolve::cardinality::classify_all(&*client, &*s, now, 5, 5).await {
            Ok(cs) => info!(
                elapsed=?t_card.elapsed(),
                pairs=cs.total, scalar=cs.scalar, list=cs.list,
                "cardinality classification done",
            ),
            Err(e) => warn!(error=%e, "cardinality classification failed; conflict detection may over-flag"),
        }
        let t_conf = std::time::Instant::now();
        let n_conf = s.detect_conflicts_bulk()?;
        info!(elapsed=?t_conf.elapsed(), conflicts=n_conf, "conflict detection done");
    }

    Ok(())
}

fn inspect(what: String, store_path: PathBuf, since: Option<i64>, limit: usize) -> Result<()> {
    let store = Store::open(&store_path)?;
    match what.as_str() {
        "conflicts" => {
            let rows = store.unresolved_conflicts()?;
            println!("{} unresolved conflicts", rows.len());
            for c in rows.iter().take(limit) {
                let a = store.fact_meta(c.fact_a_id)?;
                let b = store.fact_meta(c.fact_b_id)?;
                println!(
                    "#{} {}/{}\n  A[{}@{:.2}]: {}\n  B[{}@{:.2}]: {}",
                    c.id, c.subject, c.predicate,
                    a.adapter, a.confidence, a.object_json,
                    b.adapter, b.confidence, b.object_json,
                );
            }
        }
        "resolutions" => {
            let rows = store.recent_resolutions(since, limit)?;
            println!("{} llm-resolve refs (limit={}{})",
                rows.len(),
                limit,
                since.map(|t| format!(", since={}", t)).unwrap_or_default(),
            );
            for r in &rows {
                let target = serde_json::from_str::<serde_json::Value>(&r.object_json)
                    .ok()
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| r.object_json.clone());
                println!(
                    "  [{:.2}] {} --{}--> {}{}",
                    r.confidence, r.subject, r.predicate, target,
                    r.locator.as_ref().map(|l| format!("   « {} »", truncate(l, 80))).unwrap_or_default(),
                );
            }
        }
        other => {
            warn!(what=%other, "unknown inspect target — try: conflicts, resolutions");
        }
    }
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max { return s.to_string(); }
    let mut out = String::new();
    for c in s.chars() {
        if out.chars().count() >= max { break; }
        out.push(c);
    }
    out.push('…');
    out
}

fn resolve_cmd(store_path: PathBuf) -> Result<()> {
    let store = Store::open(&store_path)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let cfg = ce_resolve::ResolverConfig::default();
    let stats = ce_resolve::run(&store, &cfg, now)?;
    info!(resolved=stats.resolved, inbox=stats.inbox, "resolve done");
    Ok(())
}

fn build_cmd(store_path: PathBuf, out: PathBuf, templates: Option<PathBuf>) -> Result<()> {
    let store = Store::open(&store_path)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let mut renderer = ce_views::Renderer::new()?;
    if let Some(dir) = templates {
        let n = ce_views::load_template_dir(&mut renderer, &dir)?;
        info!(templates=n, "loaded");
    }
    let stats = ce_views::build_all(&store, &renderer, &out, now)?;
    info!(written=stats.written, skipped=stats.skipped, out=%out.display(), "build done");
    Ok(())
}

fn index_cmd(store_path: PathBuf, idx: PathBuf) -> Result<()> {
    let store = Store::open(&store_path)?;
    let n = ce_search::build_index(&store, &idx)?;
    info!(docs = n, "index done");
    Ok(())
}

fn search_cmd(text: String, store_path: PathBuf, idx: PathBuf, k: usize, hops: usize) -> Result<()> {
    let hits = ce_search::search(&idx, &text, k)?;
    println!("{} hits", hits.len());
    for h in &hits {
        println!("  [{:.3}] {}/{}", h.score, h.entity_type, h.id);
    }
    if hops > 0 {
        let store = Store::open(&store_path)?;
        let graph = ce_search::build_graph(&store)?;
        let seeds: Vec<String> = hits.iter().map(|h| h.id.clone()).collect();
        let related = ce_search::expand(&graph, &seeds, hops);
        println!("\n{} related (within {} hops)", related.len(), hops);
        for (id, t, d) in related.iter().take(50) {
            println!("  d={} {}/{}", d, t, id);
        }
    }
    Ok(())
}

fn agg_cmd(kind: AggKind) -> Result<()> {
    match kind {
        AggKind::EntityCounts { store } => {
            let s = Store::open(&store)?;
            for (t, n) in s.entity_counts()? {
                println!("{:>6}  {}", n, t);
            }
        }
        AggKind::PredicateCounts { r#type, store } => {
            let s = Store::open(&store)?;
            for (p, n) in s.predicate_counts(r#type.as_deref())? {
                println!("{:>6}  {}", n, p);
            }
        }
        AggKind::TopValues { predicate, limit, store } => {
            let s = Store::open(&store)?;
            for (v, n) in s.top_values(&predicate, limit)? {
                println!("{:>6}  {}", n, v);
            }
        }
    }
    Ok(())
}

fn list_cmd(etype: String, store_path: PathBuf, contains: Option<String>, limit: Option<usize>) -> Result<()> {
    let store = Store::open(&store_path)?;
    let by_type = store.entities_by_type()?;
    let Some(ids) = by_type.get(&etype) else {
        eprintln!("no entities of type '{}'. known types:", etype);
        for (t, n) in store.entity_counts()? {
            eprintln!("  {:>6}  {}", n, t);
        }
        std::process::exit(1);
    };
    let needle = contains.as_deref().map(|s| s.to_lowercase());
    let mut filtered: Vec<&String> = ids.iter()
        .filter(|id| match &needle {
            Some(n) => id.to_lowercase().contains(n),
            None => true,
        })
        .collect();
    filtered.sort();
    let total = filtered.len();
    let shown = limit.map(|l| filtered.len().min(l)).unwrap_or(filtered.len());
    println!("{} {} ({} shown)", total, etype, shown);
    for id in filtered.iter().take(shown) {
        println!("{}", id);
    }
    Ok(())
}

fn show_cmd(etype: String, id: String, store_path: PathBuf, format: String) -> Result<()> {
    let store = Store::open(&store_path)?;
    let facts = store.facts_for_subject(&id)?;
    let exists = facts.iter().any(|_| true) || store
        .all_entities()?
        .iter()
        .any(|(eid, et)| eid == &id && et == &etype);
    if !exists {
        eprintln!("entity {}/{} not found", etype, id);
        std::process::exit(1);
    }
    match format.as_str() {
        "md" => {
            let renderer = ce_views::Renderer::new()?;
            let aliases = store.aliases_for_subject(&id)?;
            let (body, _) = renderer.render_with_context(&id, &etype, &facts, &[], &aliases)?;
            println!("{}", body);
        }
        "json" => {
            let arr: Vec<serde_json::Value> = facts.iter().map(|f| {
                let obj: serde_json::Value = serde_json::from_str(&f.object_json)
                    .unwrap_or(serde_json::Value::String(f.object_json.clone()));
                serde_json::json!({
                    "predicate": f.predicate,
                    "object": obj,
                    "adapter": f.adapter,
                    "confidence": f.confidence,
                    "locator": f.locator,
                })
            }).collect();
            let out = serde_json::json!({
                "id": id, "entity_type": etype,
                "fact_count": facts.len(), "facts": arr,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        _ => {
            println!("# {}/{}  ({} facts)", etype, id, facts.len());
            let mut refs = Vec::new();
            let mut attrs = Vec::new();
            for f in &facts {
                if f.predicate.starts_with("ref:") { refs.push(f); } else { attrs.push(f); }
            }
            for f in &attrs {
                let obj: serde_json::Value = serde_json::from_str(&f.object_json)
                    .unwrap_or(serde_json::Value::String(f.object_json.clone()));
                let val = match &obj {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                println!("  {:30} {}  [{}@{:.2}]", f.predicate, val, f.adapter, f.confidence);
            }
            if !refs.is_empty() {
                println!("\n## related");
                for f in &refs {
                    let obj: serde_json::Value = serde_json::from_str(&f.object_json)
                        .unwrap_or(serde_json::Value::String(f.object_json.clone()));
                    let val = match &obj {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    println!("  {:30} {}  [{}@{:.2}]", f.predicate, val, f.adapter, f.confidence);
                }
            }
        }
    }
    Ok(())
}

fn alias_index_cmd(store_path: PathBuf, rebuild: bool) -> Result<()> {
    let mut store = Store::open(&store_path)?;
    if rebuild { store.clear_aliases()?; }
    let stats = derive_aliases_from_store(&mut store)?;
    info!(
        entities = stats.entities,
        aliases = stats.aliases_written,
        alias_fields = ?stats.fields_used,
        "alias-index done",
    );
    Ok(())
}

async fn cleanup_conflicts_cmd(
    store_path: PathBuf,
    llm_config: Option<PathBuf>,
    reclassify: bool,
) -> Result<()> {
    let store = Store::open(&store_path)?;
    let cfg = LlmConfig::load(llm_config.as_deref())
        .map_err(|e| anyhow::anyhow!("llm config: {}", e))?;
    let client = build_chat_client(&cfg).map_err(|e| anyhow::anyhow!("llm client: {}", e))?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

    if reclassify {
        let n: i64 = store.conn.query_row(
            "SELECT count(*) FROM predicate_cardinality",
            [],
            |r| r.get(0),
        )?;
        store.conn.execute("DELETE FROM predicate_cardinality", [])?;
        store.conn.execute(
            "DELETE FROM conflicts WHERE resolved_at IS NULL",
            [],
        )?;
        info!(cleared = n, "wiped cardinality cache + unresolved conflicts");
    }

    let cs = ce_resolve::cardinality::classify_all(&*client, &store, now, 5, 5)
        .await
        .map_err(|e| anyhow::anyhow!("classify: {}", e))?;
    info!(pairs=cs.total, scalar=cs.scalar, list=cs.list, "cardinality classification done");

    let n_conf = store.detect_conflicts_bulk()?;
    info!(conflicts = n_conf, "conflict detection done");
    Ok(())
}

fn reconcile_refs_cmd(store_path: PathBuf, discount: f32, rebuild: bool) -> Result<()> {
    let store = Store::open(&store_path)?;
    if rebuild {
        let n = store.clear_alias_reconcile_facts()?;
        info!(dropped = n, "cleared prior alias-reconcile facts");
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let stats = reconcile_cross_schema_refs(&store, now, discount)?;
    info!(
        scanned = stats.facts_scanned,
        emitted = stats.refs_emitted,
        mention_refs = stats.mention_refs,
        ambiguous = stats.ambiguous_skipped,
        "reconcile-refs done",
    );
    Ok(())
}

async fn embed_cmd(
    store_path: PathBuf,
    llm_config: Option<PathBuf>,
    batch_size: usize,
    force: bool,
    limit: Option<usize>,
) -> Result<()> {
    let cfg = LlmConfig::load(llm_config.as_deref())
        .map_err(|e| anyhow::anyhow!("llm config: {}", e))?;
    let embed = build_embed_client(&cfg).map_err(|e| anyhow::anyhow!("embed client: {}", e))?;
    let mut store = Store::open(&store_path)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    info!(provider=?cfg.provider, model=%embed.model(), batch=batch_size, force, limit=?limit, "embed pass starting");
    let stats = run_entity_embed_pass(&mut store, &*embed, batch_size, force, now, limit)
        .await
        .map_err(|e| anyhow::anyhow!("embed: {}", e))?;
    info!(
        considered = stats.considered,
        embedded = stats.embedded,
        skipped_unchanged = stats.skipped_unchanged,
        batches = stats.batches,
        "embed pass done",
    );
    Ok(())
}

async fn llm_test_cmd(llm_config: Option<PathBuf>, prompt: String) -> Result<()> {
    let cfg = LlmConfig::load(llm_config.as_deref())
        .map_err(|e| anyhow::anyhow!("llm config: {}", e))?;
    info!(provider=?cfg.provider, model=%cfg.model, base_url=%cfg.effective_base_url(), "smoke test");
    let client = build_chat_client(&cfg).map_err(|e| anyhow::anyhow!("client: {}", e))?;
    let out = client.chat_json(
        "You are a JSON-only echo. Always reply with a single JSON object.",
        &prompt,
    ).await.map_err(|e| anyhow::anyhow!("chat: {}", e))?;
    let preview: String = out.chars().take(200).collect();
    println!("{}", preview);
    Ok(())
}

async fn serve_cmd(
    store: PathBuf,
    idx: Option<PathBuf>,
    addr: String,
    md_store: PathBuf,
    no_md_cache: bool,
) -> Result<()> {
    let socket: std::net::SocketAddr = addr.parse()?;
    let md_root = if no_md_cache { None } else { Some(md_store) };
    let state = ce_api::AppState::new(store, idx, md_root)?;
    ce_api::serve(socket, state).await?;
    Ok(())
}

/// Ask the LLM to rank entity types by business importance, most important
/// first. Returns the type names in priority order; missing types from the
/// model's reply are appended in count-desc order so nothing is dropped.
async fn rank_entity_types(
    client: &dyn ce_llm::ChatClient,
    type_counts: &[(String, i64)],
) -> Result<Vec<String>> {
    if type_counts.is_empty() { return Ok(Vec::new()); }
    let listing: String = type_counts
        .iter()
        .map(|(t, n)| format!("- {} ({} rows)", t, n))
        .collect::<Vec<_>>()
        .join("\n");
    let user = format!(
        "Entity types in this knowledge graph, with row counts:\n{}\n\n\
        Order them by business importance for a human reader: high-signal \
        master records (customers, accounts, products, employees, orders, \
        contracts, …) BEFORE low-signal bulk (chat / message lines, raw log \
        rows, generic 'document' or 'section' types). Use the type *names* \
        themselves to judge — no other context.\n\n\
        Reply with strict JSON: {{\"order\": [\"type1\", \"type2\", ...]}}. \
        Include every type from the list, exactly once, no extras.",
        listing,
    );
    let raw = client.chat_json(
        "You are a JSON-only ranker. Reply with one JSON object, no prose.",
        &user,
    ).await.map_err(|e| anyhow::anyhow!("rank chat: {}", e))?;

    let v: serde_json::Value = serde_json::from_str(raw.trim())
        .map_err(|e| anyhow::anyhow!("rank parse ({}): {}", e, raw.chars().take(200).collect::<String>()))?;
    let arr = v.get("order")
        .and_then(|o| o.as_array())
        .ok_or_else(|| anyhow::anyhow!("rank: missing `order` array in {}", raw.chars().take(200).collect::<String>()))?;

    let known: std::collections::HashSet<&str> =
        type_counts.iter().map(|(t, _)| t.as_str()).collect();
    let mut seen: std::collections::HashSet<String> = Default::default();
    let mut order: Vec<String> = Vec::with_capacity(type_counts.len());
    for t in arr {
        if let Some(s) = t.as_str() {
            if known.contains(s) && seen.insert(s.to_string()) {
                order.push(s.to_string());
            }
        }
    }
    // Append any types the model forgot, count-desc, so they still get rendered.
    let mut leftover: Vec<&(String, i64)> = type_counts.iter()
        .filter(|(t, _)| !seen.contains(t))
        .collect();
    leftover.sort_by(|a, b| b.1.cmp(&a.1));
    for (t, _) in leftover { order.push(t.clone()); }
    Ok(order)
}

async fn gen_md_cmd(
    store_path: PathBuf,
    md_store: PathBuf,
    llm_config: Option<PathBuf>,
    etype_filter: Option<String>,
    limit: Option<usize>,
    concurrency: usize,
    max_retries: usize,
    force: bool,
) -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let cfg = LlmConfig::load(llm_config.as_deref())
        .map_err(|e| anyhow::anyhow!("llm config: {}", e))?;
    let store = Store::open(&store_path)?;
    let entities = store.all_entities()?;
    let type_counts = store.entity_counts()?;
    drop(store); // each render task opens its own connection

    std::fs::create_dir_all(&md_store)?;

    // Filter by --etype, then drop already-cached unless --force.
    let mut work: Vec<(String, String)> = entities
        .into_iter()
        .map(|(id, etype)| (etype, id))
        .filter(|(etype, _)| etype_filter.as_deref().map_or(true, |t| t == etype))
        .collect();
    let total_before = work.len();
    if !force {
        work.retain(|(etype, id)| {
            !ce_api::md_cache::cache_path(&md_store, etype, id).exists()
        });
    }
    let cached_skipped = total_before - work.len();

    // Ask the LLM to rank entity types by business importance — render the
    // high-signal stuff (customers, accounts, …) before low-signal bulk
    // (chat lines, log rows). Schema-agnostic: model decides from type
    // names + counts, no hardcoded list.
    let ranking_chat = build_chat_client(&cfg).map_err(|e| anyhow::anyhow!("llm client: {}", e))?;
    let order = rank_entity_types(&*ranking_chat, &type_counts).await
        .unwrap_or_else(|e| {
            warn!(error=%e, "type ranking failed; falling back to count-desc order");
            let mut t = type_counts.clone();
            t.sort_by(|a, b| b.1.cmp(&a.1));
            t.into_iter().map(|(t, _)| t).collect()
        });
    info!(?order, "render priority");
    let rank_of: std::collections::HashMap<String, usize> = order
        .iter().enumerate().map(|(i, t)| (t.clone(), i)).collect();
    let last = order.len();
    work.sort_by(|a, b| {
        let ra = rank_of.get(&a.0).copied().unwrap_or(last);
        let rb = rank_of.get(&b.0).copied().unwrap_or(last);
        ra.cmp(&rb).then_with(|| a.1.cmp(&b.1))
    });

    if let Some(n) = limit { work.truncate(n); }
    let total = work.len();
    info!(
        total_entities = total_before,
        cached_skipped,
        to_render = total,
        concurrency,
        max_retries,
        md_store = %md_store.display(),
        "gen-md starting",
    );
    if total == 0 {
        return Ok(());
    }

    let pb = Arc::new(ProgressBar::new(total as u64));
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] [{wide_bar:.green/dim}] {pos}/{len} ({percent}%) · ETA {eta_precise} · {msg}",
        )
        .unwrap()
        .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(250));

    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let store_path = Arc::new(store_path);
    let md_store = Arc::new(md_store);
    let cfg = Arc::new(cfg);
    let ok = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(total);
    for (etype, id) in work {
        let sem = sem.clone();
        let store_path = store_path.clone();
        let md_store = md_store.clone();
        let cfg = cfg.clone();
        let pb = pb.clone();
        let ok = ok.clone();
        let fail = fail.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            // Exponential backoff with jitter. Cap at 60s so a stuck provider
            // doesn't park a worker forever — the retry budget runs out instead.
            let mut last_err: Option<String> = None;
            for attempt in 0..=max_retries {
                let res = ce_api::render_entity_markdown(
                    store_path.as_ref(),
                    cfg.as_ref(),
                    &etype,
                    &id,
                    Some(md_store.as_ref()),
                ).await;
                match res {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e.to_string());
                        if attempt == max_retries { break; }
                        let base_ms = 1000u64.saturating_mul(1 << attempt.min(6));
                        let base_ms = base_ms.min(60_000);
                        // Cheap jitter: hash (id, attempt) → 0.8..1.2× factor.
                        let mut h: u64 = 1469598103934665603;
                        for b in id.as_bytes() { h ^= *b as u64; h = h.wrapping_mul(1099511628211); }
                        h ^= attempt as u64;
                        let jitter = 800 + (h % 401); // 800..=1200
                        let sleep_ms = base_ms.saturating_mul(jitter) / 1000;
                        tracing::warn!(etype=%etype, id=%id, attempt, sleep_ms, error=%last_err.as_deref().unwrap_or(""), "gen-md: retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    }
                }
            }
            if let Some(e) = last_err {
                fail.fetch_add(1, Ordering::Relaxed);
                tracing::error!(etype=%etype, id=%id, error=%e, "gen-md: gave up");
            }
            let o = ok.load(Ordering::Relaxed);
            let f = fail.load(Ordering::Relaxed);
            pb.set_message(format!("ok={} fail={}", o, f));
            pb.inc(1);
        }));
    }

    for h in handles {
        if let Err(e) = h.await { warn!(error=%e, "gen-md task join failed"); }
    }
    pb.finish_with_message("done");
    info!(
        ok = ok.load(Ordering::Relaxed),
        fail = fail.load(Ordering::Relaxed),
        "gen-md done",
    );
    Ok(())
}

/// Stable content-hash for delta detection. SHA-256 over the file bytes.
/// Stable across runs and OSes — required so re-ingest can detect "nothing changed".
fn file_hash(path: &PathBuf) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path)?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Ok(hex::encode(h.finalize()))
}
