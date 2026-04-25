# Qontext — Rust Implementation Plan

Companion to `QONTEXT_PLAN.md`. Rust-side spec.

---

## 1. Spine: evmap shared state

One `evmap<String, Arc<Node>>`. Many readers (FUSE, HTTP, WS). One writer (Builder). Lock-free reads.

```rust
// crates/core/src/state.rs
static FACTORY: OnceLock<ReadHandleFactory<NodePath, Node>> = OnceLock::new();
static WRITER : OnceLock<Mutex<WriteHandle<NodePath, Node>>> = OnceLock::new();

pub fn init();
pub fn reader() -> ReadHandle<NodePath, Node>;            // per-thread clone
pub fn read_node(path: &str) -> Option<Arc<Node>>;
pub fn upsert_node(path: NodePath, node: Node);           // single + refresh
pub fn upsert_batch(items: impl IntoIterator<Item=(NodePath, Node)>); // batch + 1 refresh
pub fn list_paths() -> Vec<NodePath>;
```

Rules:
- `init()` exactly once per process, before any reader/writer thread.
- Each reader thread calls `reader()` for its own handle. Never share a `ReadHandle`.
- Writer mutex is uncontended (one builder task). Guards solo-dev binaries.
- `wh.refresh()` is the cost — batch first, refresh once.

Fallback (decide h1): `arc-swap = "1"` swapping a whole `Arc<HashMap<...>>` if evmap derive misbehaves. O(n) writes, simpler invariants.

---

## 2. Crate layout

```
crates/
├── core/        Node, evmap state, ids, path utils. NO LLM, NO providers.
├── fs/          FUSE3. Reads core. Linux only.
├── api/         axum HTTP + WS. Reads core.
├── builder/     Watches /source, providers, LLM, graph, render. Writes core.
└── qontext/     Orchestrator: state::init() once + spawns all 3.
```

**Dependency rule:** every binary depends on `qontext-core` and nothing else cross-crate. `qontext-fs` and `qontext-api` must not depend on `qontext-builder`. CI assert later (cargo metadata script).

Each binary independently runnable: `cargo run -p qontext-fs`, `cargo run -p qontext-api`, `cargo run -p qontext-builder`. Solo dev never blocks on the other two.

---

## 3. Node — minimal shared type

```rust
// crates/core/src/model.rs
pub enum NodeKind { File, Dir, Link }

pub struct FileNode {                                 // kept FileNode for less churn
    pub kind:       NodeKind,
    pub content:    String,
    pub children:   Vec<String>,                      // absolute paths
    pub size:       u64,
    pub mtime_secs: u64,
    pub etag:       u64,
    pub meta:       BTreeMap<String, String>,         // builder-defined tags
}
```

`Eq + Hash` required by evmap. `BTreeMap<String,String>` satisfies both (deterministic iteration). Floats banned (use `u32 = x*100`).

No `provenance`, no `fact_id`, no conflict types in core. Builder serializes its richer concepts via `meta` keys (`entity_type`, `entity_id`, `link_target`, `source_uri`, `confidence_x100`, `resolved_by`, `human_notes`, ...).

---

## 4. Builder

```
crates/builder/
├── src/
│   ├── lib.rs            pub run(source_dir, options)
│   ├── source.rs         notify::recommended_watcher recursive + drain
│   ├── events.rs         SourceEvent { path, kind: Create|Modify|Delete }
│   ├── providers/
│   │   ├── mod.rs        trait Provider, Registry, Entry
│   │   ├── json.rs
│   │   ├── csv.rs
│   │   ├── markdown.rs
│   │   └── pdf.rs        text-only via pdf-extract
│   ├── extractor.rs      LLM: Entry → Vec<EntityCandidate>
│   ├── resolver.rs       alias table + LLM tiebreak
│   ├── relater.rs        LLM: Vec<Edge> from graph + new entities
│   ├── graph.rs          Graph { entities, edges }; in-memory; snapshot to ./.qontext/graph.json
│   ├── render.rs         Graph → Vec<(NodePath, FileNode)>; one batch upsert per change set
│   ├── llm.rs            reqwest → Anthropic /v1/messages + cache_control: ephemeral
│   ├── processor.rs      main loop: SourceEvent → providers → extractor → resolver → relater → render
│   └── main.rs
└── prompts/              include_str!() templates
    ├── extract.md
    ├── resolve.md
    └── relate.md
```

### 4.1 Provider trait

```rust
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn matches(&self, path: &Path) -> bool;
    fn fetch(&self, path: &Path) -> Result<Vec<Entry>>;
    fn list(&self, root: &Path)  -> Result<Vec<Entry>> { /* default: walk + fetch */ }
    fn search(&self, _q: &str, _root: &Path) -> Result<Vec<Entry>> { Ok(vec![]) }
}

pub struct Entry {
    pub id:   String,
    pub kind: String,
    pub text: String,
    pub meta: BTreeMap<String, String>,
}

pub struct Registry { providers: Vec<Box<dyn Provider>> }
impl Registry {
    pub fn with_default() -> Self;                    // registers stock providers
    pub fn register(&mut self, p: Box<dyn Provider>);
    pub fn for_path<'a>(&'a self, p: &Path) -> Option<&'a dyn Provider>;
}
```

Stock providers register at startup. Adding a new format = one file + one push into the registry. No other layer aware.

### 4.2 Source watch

```rust
// builder/src/source.rs
pub fn watch_blocking(root: &Path, tx: mpsc::Sender<SourceEvent>) -> Result<()>;
```

`notify::recommended_watcher` recursive on `root`. On Create/Modify/Delete: push `SourceEvent { path, kind }`. Drain existing files at startup so cold start indexes everything.

### 4.3 Pipeline

```rust
async fn handle(ev: SourceEvent, st: &BuilderState) -> Result<()> {
    let prov = st.registry.for_path(&ev.path).context("no provider")?;
    let entries  = prov.fetch(&ev.path)?;
    let cands    = extractor::extract_many(&entries).await?;
    let resolved = resolver::resolve_all(cands, &st.graph).await?;
    st.graph.upsert_entities(resolved);
    let edges    = relater::relate(&entries, &st.graph).await?;
    st.graph.upsert_edges(edges);
    let updates  = render::project(&st.graph, /* affected ids */);
    qontext_core::state::upsert_batch(updates);       // 1 refresh
    if let Some(tx) = &st.notify { let _ = tx.send(updates_paths(&updates)); }
    Ok(())
}
```

Hash-cache by `(path, sha)`. Skip unchanged.

### 4.4 LLM client

`reqwest` + Anthropic Messages API. ~50 lines. `cache_control: ephemeral` on the system prompt — same prompt fires hundreds of times, cache pays off after h6. Models: `claude-haiku-4-5` for extract/relate, `claude-sonnet-4-6` for resolver tiebreaks only.

---

## 5. FUSE3 daemon (`crates/fs`)

Same pattern as before. Path → inode via `crates/core/src/ids.rs`. Implements `Filesystem::lookup, getattr, read, readdir`. Read-only — `write` returns `EROFS`. Linux-gated (`#[cfg(target_os = "linux")]`); on others returns a friendly error so other crates still build on macOS.

The FS layer is **format-agnostic**. It serves whatever bytes are in `Node.content`. Markdown rendering is the builder's responsibility.

---

## 6. HTTP API (`crates/api`)

Routes:

```
GET  /healthz
GET  /api/tree?prefix=/customers
GET  /api/node/*path                  → { kind, content, children, meta, size, etag, mtime_secs }
GET  /api/query?entity=&id=&edge=     → [Node]  (resolves edge subdir → child Nodes)
GET  /api/search?q=...                → [Node]  (substring over content + meta)
GET  /api/events                      → WS, broadcasts {path, etag} after each refresh
GET  /api/stats
POST /api/source                      → write JSON body to ./source/{path}
```

`/api/query` and `/api/search` are pure reads of `core::state`. No builder dependency. WS publishes from a `tokio::sync::broadcast` channel that the orchestrator wires to the builder. Standalone `qontext-api` creates its own (no-op) channel — harmless.

---

## 7. Orchestrator (`crates/qontext`)

```rust
fn main() {
    qontext_core::state::init();
    let (notify_tx, _notify_rx) = tokio::sync::broadcast::channel(256);

    tokio::spawn(qontext_builder::run(source_dir, BuilderOpts::default().with_notify(notify_tx.clone())));
    tokio::spawn(qontext_api::run(addr, ApiOpts::default().with_notify(notify_tx)));

    // FUSE blocks → dedicated thread
    std::thread::spawn(move || { let _ = qontext_fs::run(&mountpoint); });
    /* wait */
}
```

One process. One evmap. Three faces.

---

## 8. Pitfalls

| Pitfall                                                     | Avoid by                                                                                  |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Heavy types creep into `core::Node`                         | PR rule: `core` may not import provider/LLM/graph types                                   |
| `qontext-fs`/`qontext-api` accidentally depend on builder   | `cargo metadata` CI script asserts the dep edge does not exist                            |
| evmap `ShallowCopy` derive surprise                         | Keep `Node` behind `Arc`; hour 0                                                          |
| Forgetting `wh.refresh()`                                   | Always go through `upsert_node` / `upsert_batch` helpers (refresh internally)             |
| Recursive notify floods events                              | Debounce 250ms in `source.rs`                                                             |
| Provider returns enormous text blob                         | Cap `Entry.text` at 32 KiB; chunk pdf/csv into multiple Entries                            |
| LLM returns invalid JSON                                    | `tool_use` schema + retry once; log + skip on second failure                              |
| Mount dir busy on dev restart                               | `MountOption::AutoUnmount`; `make unmount` wraps `fusermount -u`                          |
| FUSE inode collisions                                       | Path → inode via incrementing counter (BiMap), not hash. Done in `core::ids`.             |
| Compile times                                               | Workspace, `cargo check`, `mold`. No `serde_with`, no `validator`, no fancy deps.         |

---

## 9. Hour-by-hour for the Rust dev

| h     | Done                                                                                                            |
|-------|-----------------------------------------------------------------------------------------------------------------|
| 0–2   | workspace; `core::Node` slim; evmap state wired; 3 binaries each say "alive"                                    |
| 2–6   | `source` watcher + drain; JsonProvider, CsvProvider, MarkdownProvider; render Entry → 1:1 mirror Nodes (no LLM) |
| 6–12  | `llm::call`; `extractor`; `resolver` (alias table only); render entity cards `/{kind}/{id}/_.md`                |
| 12–18 | LLM `relater`; edge subdirs render; `/api/query`; WS push wired                                                 |
| 18–22 | PdfProvider; auto-update on source modify; graph snapshot to disk; `/api/search`                                |
| 22–24 | bug hunt; demo run × 3; video backup                                                                            |

---

## 10. Done when

- `./source/` has fixtures across ≥3 formats. `cargo run -p qontext` mounts `/mnt/qontext` and `ls customers/acme-co/orders` shows order files.
- Edit a CSV row → within 3s the affected employee card updates; orphan edges cleaned up.
- `curl /api/query?entity=customer&id=acme-co&edge=orders` returns the same set as the mount.
- `cargo run -p qontext-fs` alone mounts (empty or seeded) tree.
- `cargo run -p qontext-api` alone serves a stub (or fixture-seeded) tree.
- `cargo run -p qontext-builder` alone fills `core::state` from `./source/` with no UI/mount needed.

End.
