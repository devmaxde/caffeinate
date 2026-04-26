use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ce_llm::LlmConfig;
use ce_search::Graph;
use ce_store::Store;
use ce_views::Renderer;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

pub mod render;
pub mod md_cache;
mod ask;

pub use render::render_entity_markdown;

pub struct AppState {
    pub store_path: PathBuf,
    pub idx_path: Option<PathBuf>,
    pub renderer: Renderer,
    pub graph: Mutex<Option<Arc<Graph>>>,
    /// Loaded once at startup so /entities/.../render.md doesn't repeat the
    /// disk read per request. None when no ce.toml + no env vars are set.
    pub llm_cfg: Option<LlmConfig>,
    /// Root of the on-disk markdown cache. Hits skip the LLM entirely; misses
    /// are written here on success so subsequent requests are free.
    pub md_store_root: Option<PathBuf>,
}

impl AppState {
    pub fn new(
        store_path: PathBuf,
        idx_path: Option<PathBuf>,
        md_store_root: Option<PathBuf>,
    ) -> Result<Arc<Self>, ApiError> {
        let renderer = Renderer::new().map_err(|e| ApiError::Internal(e.to_string()))?;
        let llm_cfg = LlmConfig::load(None).ok();
        // Drop a missing index path so callers (`/retrieve`, `/ask` search tool)
        // don't expose features that will fail at runtime. Tantivy stores its
        // index as a directory.
        let idx_path = idx_path.and_then(|p| {
            if p.exists() {
                Some(p)
            } else {
                tracing::warn!(path=%p.display(), "index path missing — search disabled (run `ce index`)");
                None
            }
        });
        if let Some(root) = md_store_root.as_ref() {
            if let Err(e) = std::fs::create_dir_all(root) {
                tracing::warn!(path=%root.display(), error=%e, "md_store: create_dir_all failed; cache disabled");
            }
        }
        Ok(Arc::new(Self {
            store_path,
            idx_path,
            renderer,
            graph: Mutex::new(None),
            llm_cfg,
            md_store_root,
        }))
    }

    fn open_store(&self) -> Result<Store, ApiError> {
        Store::open(&self.store_path).map_err(|e| ApiError::Internal(format!("store open: {e}")))
    }

    async fn graph(&self) -> Result<Arc<Graph>, ApiError> {
        let mut g = self.graph.lock().await;
        if let Some(existing) = g.as_ref() {
            return Ok(existing.clone());
        }
        let store = self.open_store()?;
        let built = ce_search::build_graph(&store)
            .map_err(|e| ApiError::Internal(format!("graph: {e}")))?;
        let arc = Arc::new(built);
        *g = Some(arc.clone());
        Ok(arc)
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index_html))
        .route("/health", get(health))
        .route("/entities", get(list_entity_types))
        .route("/entities/:etype", get(list_entities_of_type))
        .route("/entities/:etype/:id", get(get_entity))
        .route("/entities/:etype/:id/render.md", get(render_entity_md))
        .route("/ask", post(ask_question))
        .route("/retrieve", post(retrieve))
        .route("/conflicts", get(list_conflicts))
        .route("/conflicts/:id", get(get_conflict))
        .route("/conflicts/:id/resolve", post(resolve_conflict))
        .route("/resolutions", get(list_resolutions))
        .route("/resolutions/approve", post(approve_resolution))
        .route("/resolutions/reject", post(reject_resolution))
        .route("/ui/conflicts", get(conflicts_html))
        .route("/ui/resolutions", get(resolutions_html))
        .route("/ui/browse", get(browse_html))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

const CONFLICTS_HTML: &str = include_str!("../static/conflicts.html");
const RESOLUTIONS_HTML: &str = include_str!("../static/resolutions.html");
const INDEX_HTML: &str = include_str!("../static/index.html");
const BROWSE_HTML: &str = include_str!("../static/browse.html");

async fn conflicts_html() -> axum::response::Html<&'static str> {
    axum::response::Html(CONFLICTS_HTML)
}

async fn resolutions_html() -> axum::response::Html<&'static str> {
    axum::response::Html(RESOLUTIONS_HTML)
}

async fn index_html() -> axum::response::Html<&'static str> {
    axum::response::Html(INDEX_HTML)
}

async fn browse_html() -> axum::response::Html<&'static str> {
    axum::response::Html(BROWSE_HTML)
}

pub async fn serve(addr: SocketAddr, state: Arc<AppState>) -> Result<(), ApiError> {
    let app = router(state);
    info!(%addr, "ce-api listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| ApiError::Internal(format!("bind: {e}")))?;
    axum::serve(listener, app)
        .await
        .map_err(|e| ApiError::Internal(format!("serve: {e}")))?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct FactOut {
    predicate: String,
    object: serde_json::Value,
    adapter: String,
    confidence: f64,
    locator: Option<String>,
    /// Human label for the target of a `ref:*` fact, when resolvable. Lets the
    /// browser render `B07JW9H4J1 → "Logitech wireless adapter"` instead of
    /// raw ids.
    #[serde(skip_serializing_if = "Option::is_none")]
    target_label: Option<String>,
    /// Entity-type of the ref target (= predicate stripped of `ref:`).
    #[serde(skip_serializing_if = "Option::is_none")]
    target_type: Option<String>,
}

#[derive(Serialize)]
struct IncomingOut {
    /// Predicate the source uses to point at this entity, e.g. `ref:employees`.
    predicate: String,
    /// Subject id of the entity that holds the ref.
    subject: String,
    /// Type of that source entity (empty if unknown).
    subject_type: String,
    /// Best human label for the source entity.
    subject_label: Option<String>,
    adapter: String,
    confidence: f64,
}

#[derive(Serialize)]
struct EntityOut {
    id: String,
    entity_type: String,
    label: Option<String>,
    facts: Vec<FactOut>,
    /// Edges *into* this entity — i.e. other entities that reference it via
    /// `ref:*`. Lets the UI render "Referenced by" / backlinks.
    incoming: Vec<IncomingOut>,
    markdown: String,
    source_count: usize,
}

#[derive(Serialize)]
struct EntityTypeOut {
    entity_type: String,
    count: i64,
}

async fn list_entity_types(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<EntityTypeOut>>, ApiError> {
    let store = state.open_store()?;
    let counts = store
        .entity_counts()
        .map_err(|e| ApiError::Internal(format!("counts: {e}")))?;
    Ok(Json(
        counts
            .into_iter()
            .map(|(t, c)| EntityTypeOut { entity_type: t, count: c })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct EntityBrief {
    id: String,
    label: Option<String>,
}

#[derive(Serialize)]
struct EntityListOut {
    entity_type: String,
    total: i64,
    offset: usize,
    limit: usize,
    items: Vec<EntityBrief>,
}

async fn list_entities_of_type(
    State(state): State<Arc<AppState>>,
    Path(etype): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ListQuery>,
) -> Result<Json<EntityListOut>, ApiError> {
    let store = state.open_store()?;
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(100).min(1000);
    let (ids, total) = store
        .entities_of_type(&etype, q.q.as_deref(), offset, limit)
        .map_err(|e| ApiError::Internal(format!("list: {e}")))?;
    let labels = store
        .entity_labels_bulk(&ids)
        .map_err(|e| ApiError::Internal(format!("labels: {e}")))?;
    let items = ids
        .into_iter()
        .map(|id| {
            let label = labels.get(&id).cloned();
            EntityBrief { id, label }
        })
        .collect();
    Ok(Json(EntityListOut {
        entity_type: etype,
        total,
        offset,
        limit,
        items,
    }))
}

#[derive(Deserialize)]
struct AskReq {
    question: String,
}

#[derive(Serialize)]
struct AskResp {
    answer: String,
}

async fn ask_question(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AskReq>,
) -> Result<Json<AskResp>, ApiError> {
    let cfg = state.llm_cfg.as_ref().ok_or_else(|| {
        ApiError::BadRequest("server has no LLM config (set ce.toml or CE_LLM_* env vars)".into())
    })?;
    let q = body.question.trim();
    if q.is_empty() {
        return Err(ApiError::BadRequest("question is empty".into()));
    }
    let answer = ask::answer_question(
        &state.store_path,
        state.idx_path.as_deref(),
        cfg,
        q,
    ).await?;
    Ok(Json(AskResp { answer }))
}

async fn render_entity_md(
    State(state): State<Arc<AppState>>,
    Path((etype, id)): Path<(String, String)>,
) -> Result<([(axum::http::header::HeaderName, &'static str); 1], String), ApiError> {
    let cfg = state.llm_cfg.as_ref().ok_or_else(|| {
        ApiError::BadRequest("server has no LLM config (set ce.toml or CE_LLM_* env vars)".into())
    })?;
    let md = render::render_entity_markdown(
        &state.store_path,
        cfg,
        &etype,
        &id,
        state.md_store_root.as_deref(),
    ).await?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/markdown; charset=utf-8")],
        md,
    ))
}

async fn get_entity(
    State(state): State<Arc<AppState>>,
    Path((etype, id)): Path<(String, String)>,
) -> Result<Json<EntityOut>, ApiError> {
    let store = state.open_store()?;
    let facts = store
        .facts_for_subject(&id)
        .map_err(|e| ApiError::Internal(format!("facts: {e}")))?;
    if facts.is_empty() {
        // Verify entity exists at all.
        let exists = store
            .all_entities()
            .map_err(|e| ApiError::Internal(format!("entities: {e}")))?
            .into_iter()
            .any(|(eid, et)| eid == id && et == etype);
        if !exists {
            return Err(ApiError::NotFound);
        }
    }
    let (markdown, source_count) = state
        .renderer
        .render_one(&id, &etype, &facts)
        .map_err(|e| ApiError::Internal(format!("render: {e}")))?;

    // Collect ref:* target ids so we can label them in one bulk lookup.
    let ref_targets: Vec<String> = facts
        .iter()
        .filter(|f| f.predicate.starts_with("ref:"))
        .filter_map(|f| {
            serde_json::from_str::<serde_json::Value>(&f.object_json)
                .ok()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
        .collect();
    let target_labels = store
        .entity_labels_bulk(&ref_targets)
        .map_err(|e| ApiError::Internal(format!("labels: {e}")))?;

    let self_label = store
        .entity_labels_bulk(&[id.clone()])
        .map_err(|e| ApiError::Internal(format!("self label: {e}")))?
        .remove(&id);

    // Backlinks: who points AT this entity via ref:*.
    let incoming_raw = store
        .incoming_refs(&id)
        .map_err(|e| ApiError::Internal(format!("incoming: {e}")))?;
    let incoming_subject_ids: Vec<String> =
        incoming_raw.iter().map(|(_, s, _, _, _)| s.clone()).collect();
    let incoming_labels = store
        .entity_labels_bulk(&incoming_subject_ids)
        .map_err(|e| ApiError::Internal(format!("incoming labels: {e}")))?;
    let incoming: Vec<IncomingOut> = incoming_raw
        .into_iter()
        .map(|(predicate, subject, subject_type, adapter, confidence)| {
            let subject_label = incoming_labels.get(&subject).cloned();
            IncomingOut {
                predicate,
                subject,
                subject_type,
                subject_label,
                adapter,
                confidence,
            }
        })
        .collect();

    let facts_out: Vec<FactOut> = facts
        .into_iter()
        .map(|f| {
            let object = serde_json::from_str::<serde_json::Value>(&f.object_json)
                .unwrap_or_else(|_| serde_json::Value::String(f.object_json.clone()));
            let (target_label, target_type) = if let Some(t) = f.predicate.strip_prefix("ref:") {
                let tid = object.as_str().map(String::from);
                let lbl = tid.as_ref().and_then(|s| target_labels.get(s).cloned());
                (lbl, Some(t.to_string()))
            } else {
                (None, None)
            };
            FactOut {
                predicate: f.predicate,
                object,
                adapter: f.adapter,
                confidence: f.confidence,
                locator: f.locator,
                target_label,
                target_type,
            }
        })
        .collect();
    Ok(Json(EntityOut {
        id,
        entity_type: etype,
        label: self_label,
        facts: facts_out,
        incoming,
        markdown,
        source_count,
    }))
}

#[derive(Deserialize)]
struct RetrieveReq {
    query: String,
    #[serde(default = "default_k")]
    k: usize,
    #[serde(default)]
    hops: usize,
}

fn default_k() -> usize {
    10
}

#[derive(Serialize)]
struct HitOut {
    id: String,
    entity_type: String,
    score: f32,
}

#[derive(Serialize)]
struct RelatedOut {
    id: String,
    entity_type: String,
    distance: usize,
}

#[derive(Serialize)]
struct RetrieveResp {
    hits: Vec<HitOut>,
    related: Vec<RelatedOut>,
}

async fn retrieve(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RetrieveReq>,
) -> Result<Json<RetrieveResp>, ApiError> {
    let idx = state
        .idx_path
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("server started without --idx".into()))?;
    let hits = ce_search::search(idx, &req.query, req.k.max(1))
        .map_err(|e| ApiError::Internal(format!("search: {e}")))?;
    let related = if req.hops > 0 && !hits.is_empty() {
        let g = state.graph().await?;
        let seeds: Vec<String> = hits.iter().map(|h| h.id.clone()).collect();
        ce_search::expand(&g, &seeds, req.hops)
            .into_iter()
            .map(|(id, t, d)| RelatedOut {
                id,
                entity_type: t,
                distance: d,
            })
            .collect()
    } else {
        Vec::new()
    };
    let hits_out = hits
        .into_iter()
        .map(|h| HitOut {
            id: h.id,
            entity_type: h.entity_type,
            score: h.score,
        })
        .collect();
    Ok(Json(RetrieveResp {
        hits: hits_out,
        related,
    }))
}

#[derive(Serialize)]
struct ConflictBrief {
    id: i64,
    subject: String,
    predicate: String,
    fact_a_id: i64,
    fact_b_id: i64,
}

async fn list_conflicts(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ConflictBrief>>, ApiError> {
    let store = state.open_store()?;
    let rows = store
        .unresolved_conflicts()
        .map_err(|e| ApiError::Internal(format!("conflicts: {e}")))?;
    let out: Vec<ConflictBrief> = rows
        .into_iter()
        .take(500) // cap UI page size
        .map(|c| ConflictBrief {
            id: c.id,
            subject: c.subject,
            predicate: c.predicate,
            fact_a_id: c.fact_a_id,
            fact_b_id: c.fact_b_id,
        })
        .collect();
    Ok(Json(out))
}

#[derive(Serialize)]
struct FactDetail {
    id: i64,
    subject: String,
    predicate: String,
    object: serde_json::Value,
    adapter: String,
    confidence: f64,
    observed_at: i64,
    locator: Option<String>,
    /// For ref:* facts, the human label of the target entity (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    target_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_type: Option<String>,
}

#[derive(Serialize)]
struct ConflictDetail {
    id: i64,
    subject: String,
    /// Type of the subject entity, e.g. `orders`. Empty if unknown.
    subject_type: String,
    /// Human label of the subject (best alias / name field).
    subject_label: Option<String>,
    predicate: String,
    fact_a: FactDetail,
    fact_b: FactDetail,
}

fn fact_to_detail(
    f: ce_store::FactMeta,
    target_labels: &std::collections::HashMap<String, String>,
) -> FactDetail {
    let object = serde_json::from_str::<serde_json::Value>(&f.object_json)
        .unwrap_or_else(|_| serde_json::Value::String(f.object_json.clone()));
    let (target_label, target_type) = if let Some(t) = f.predicate.strip_prefix("ref:") {
        let lbl = object
            .as_str()
            .and_then(|s| target_labels.get(s).cloned());
        (lbl, Some(t.to_string()))
    } else {
        (None, None)
    };
    FactDetail {
        id: f.id,
        subject: f.subject,
        predicate: f.predicate,
        object,
        adapter: f.adapter,
        confidence: f.confidence,
        observed_at: f.observed_at,
        locator: f.locator,
        target_label,
        target_type,
    }
}

async fn get_conflict(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<ConflictDetail>, ApiError> {
    let store = state.open_store()?;
    let rows = store
        .unresolved_conflicts()
        .map_err(|e| ApiError::Internal(format!("conflicts: {e}")))?;
    let row = rows
        .into_iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::NotFound)?;
    let a = store
        .fact_meta(row.fact_a_id)
        .map_err(|e| ApiError::Internal(format!("fact a: {e}")))?;
    let b = store
        .fact_meta(row.fact_b_id)
        .map_err(|e| ApiError::Internal(format!("fact b: {e}")))?;

    // Bulk-label any ref:* targets and the conflict's subject so the UI can
    // show "B07JW9H4J1 — Logitech wireless adapter" instead of bare ids.
    let mut to_label: Vec<String> = Vec::new();
    to_label.push(row.subject.clone());
    for fm in [&a, &b] {
        if fm.predicate.starts_with("ref:") {
            if let Ok(serde_json::Value::String(s)) =
                serde_json::from_str::<serde_json::Value>(&fm.object_json)
            {
                to_label.push(s);
            }
        }
    }
    let labels = store
        .entity_labels_bulk(&to_label)
        .map_err(|e| ApiError::Internal(format!("labels: {e}")))?;
    let subject_label = labels.get(&row.subject).cloned();

    let subject_type = store
        .entity_type_of(&row.subject)
        .map_err(|e| ApiError::Internal(format!("type: {e}")))?
        .unwrap_or_default();

    Ok(Json(ConflictDetail {
        id: row.id,
        subject: row.subject,
        subject_type,
        subject_label,
        predicate: row.predicate,
        fact_a: fact_to_detail(a, &labels),
        fact_b: fact_to_detail(b, &labels),
    }))
}

#[derive(Deserialize)]
struct ResolveBody {
    /// "a" or "b": which fact to keep. Generic — caller chooses, no source-priority assumption.
    prefer: String,
}

#[derive(Serialize)]
struct ResolveResp {
    resolved: bool,
    chosen_fact_id: i64,
}

async fn resolve_conflict(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<ResolveBody>,
) -> Result<Json<ResolveResp>, ApiError> {
    let store = state.open_store()?;
    let rows = store
        .unresolved_conflicts()
        .map_err(|e| ApiError::Internal(format!("conflicts: {e}")))?;
    let row = rows
        .into_iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::NotFound)?;
    let chosen = match body.prefer.as_str() {
        "a" => row.fact_a_id,
        "b" => row.fact_b_id,
        _ => return Err(ApiError::BadRequest("prefer must be 'a' or 'b'".into())),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    store
        .resolve_conflict(row.id, &format!("prefer:{}", chosen), now)
        .map_err(|e| ApiError::Internal(format!("resolve: {e}")))?;
    Ok(Json(ResolveResp {
        resolved: true,
        chosen_fact_id: chosen,
    }))
}

// ---- Resolutions (Phase H.3) ----

#[derive(Deserialize)]
struct ResolutionsQuery {
    #[serde(default)]
    since: Option<i64>,
    #[serde(default = "default_resolutions_limit")]
    limit: usize,
    /// Only show resolutions at or below this confidence. Default 1.0 (all).
    #[serde(default = "default_max_conf")]
    max_conf: f64,
}

fn default_resolutions_limit() -> usize { 100 }
/// Auto-approve threshold: ≥0.9-conf LLM resolutions land at full confidence
/// and are considered trusted. The inbox shows everything *below* this so
/// users only ever see ambiguous cases that need a human decision.
fn default_max_conf() -> f64 { 0.9 }

#[derive(Serialize)]
struct ResolutionOut {
    subject: String,
    predicate: String,
    target: String,
    object_json: String,
    confidence: f64,
    locator: Option<String>,
    observed_at: i64,
}

async fn list_resolutions(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<ResolutionsQuery>,
) -> Result<Json<Vec<ResolutionOut>>, ApiError> {
    let store = state.open_store()?;
    let rows = store
        .recent_resolutions(q.since, q.limit)
        .map_err(|e| ApiError::Internal(format!("resolutions: {e}")))?;
    let out: Vec<ResolutionOut> = rows
        .into_iter()
        // Exclude already-rejected (suppressed) refs (conf=0) AND auto-approved
        // ones (conf ≥ max_conf). Strict `<` so a max_conf of 0.9 truly hides
        // facts that landed at exactly 0.9.
        .filter(|r| r.confidence > 0.0 && r.confidence < q.max_conf)
        .map(|r| {
            let target = serde_json::from_str::<serde_json::Value>(&r.object_json)
                .ok()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| r.object_json.clone());
            ResolutionOut {
                subject: r.subject,
                predicate: r.predicate,
                target,
                object_json: r.object_json,
                confidence: r.confidence,
                locator: r.locator,
                observed_at: r.observed_at,
            }
        })
        .collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
struct ResolutionAction {
    /// Document / subject the LLM emitted the ref:* fact under (used for delete-by-triple).
    subject: String,
    /// `ref:<type>` predicate to act on.
    predicate: String,
    /// JSON-encoded object — the resolved entity id as it sits in the facts table.
    object_json: String,
    /// Resolved entity id (target of the ref). Same as `object_json` parsed as string.
    target_id: String,
    /// Surface form from the original text (locator / evidence span). Becomes
    /// the alias on approve, the blocklist key on reject.
    surface: String,
}

#[derive(Serialize)]
struct ApproveResp { approved: bool, alias_inserted: usize }

async fn approve_resolution(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ResolutionAction>,
) -> Result<Json<ApproveResp>, ApiError> {
    let mut store = state.open_store()?;
    let norm = ce_store::normalize_alias(&body.surface);
    if norm.len() < 2 {
        return Err(ApiError::BadRequest("surface form is too short to alias".into()));
    }
    // Approving promotes the resolved (surface → entity) pair to a high-conf
    // derived alias so future runs hit it on the cheap path.
    let row = ce_store::AliasRow {
        entity_id: body.target_id.clone(),
        alias: body.surface.clone(),
        alias_norm: norm,
        source: "derived".into(),
        confidence: 1.0,
    };
    let n = store
        .bulk_upsert_aliases(&[row])
        .map_err(|e| ApiError::Internal(format!("alias upsert: {e}")))?;
    Ok(Json(ApproveResp { approved: true, alias_inserted: n }))
}

#[derive(Serialize)]
struct RejectResp { rejected: bool, fact_suppressed: usize }

async fn reject_resolution(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ResolutionAction>,
) -> Result<Json<RejectResp>, ApiError> {
    let store = state.open_store()?;
    let norm = ce_store::normalize_alias(&body.surface);
    if norm.is_empty() {
        return Err(ApiError::BadRequest("surface form is empty".into()));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    store
        .add_resolution_block(&norm, &body.target_id, now)
        .map_err(|e| ApiError::Internal(format!("blocklist: {e}")))?;
    let n = store
        .suppress_resolution_fact(&body.subject, &body.predicate, &body.object_json)
        .map_err(|e| ApiError::Internal(format!("suppress fact: {e}")))?;
    Ok(Json(RejectResp { rejected: true, fact_suppressed: n }))
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::Internal(m) => {
                error!(error=%m, "api internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
        };
        (status, Json(serde_json::json!({"error": msg}))).into_response()
    }
}
