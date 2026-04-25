# Qontext Plan

**Pitch:** Watch a folder of company data. Per-format providers extract structure. AI builds a dynamic knowledge graph. Output is a virtual filesystem where every entity (customer, employee, project) is queryable — `customers/acme/orders/` returns the orders.

---

## 1. Surface

Two paths matter:

- **`./source/`** — real on-disk folder. Source of truth. Nested by domain (`crm/`, `hr/`, `policies/`...). Files in any format the providers understand. Edits here drive everything.
- **`./output/` (mount: `/mnt/qontext/`)** — virtual, in-memory, FUSE-served. One subfolder per entity kind. Edges become subfolders.

```
/mnt/qontext/
├── customers/
│   └── acme-co/
│       ├── _.md                 entity card
│       ├── orders/{id}.md       edges as subdirs
│       └── contacts/{id}.md
├── employees/
│   └── jane-smith/
│       ├── _.md
│       ├── reports_to/{id}.md
│       ├── owns_projects/{id}.md
│       └── tickets/{id}.md
├── projects/{id}/...
└── _graph/
    └── edges.jsonl              audit + agent grep target
```

The mount, the HTTP API, and the WS push all expose the **same** in-memory tree. One shared state, three faces.

---

## 2. Architecture

```
        source/                                   /mnt/qontext
          │                                            ▲
   notify │ recursive                                  │ FUSE3
          ▼                                            │
      Builder ── Providers ──▶ Graph ──▶ Render ──▶ core::state ◀── HTTP API
          │                                            │
          └─ LLM extract / resolve / relate            └─ WS push
```

| Component | Crate / Bin       | Job                                                                 | Standalone? |
|-----------|-------------------|---------------------------------------------------------------------|-------------|
| Builder   | `qontext-builder` | Watch `source/`, dispatch providers, LLM → graph, render Nodes      | Yes — writes to core only |
| FUSE      | `qontext-fs`      | Mount `core::state` view as a filesystem (read-only)                | Yes — reads core. Empty if no builder. |
| API       | `qontext-api`     | HTTP + WS over `core::state`                                        | Yes — reads core. Same. |
| Orch.     | `qontext`         | One process: `state::init()` once, spawns the 3                     | — |

**Hard rule: no cross-component imports beyond `core`.** `qontext-fs` and `qontext-api` must not depend on `qontext-builder`. Each binary inits its own state, optionally seeds fixtures, runs its loop. `cargo run -p <one>` always works.

---

## 3. Providers

Defined inside `qontext-builder`. One trait, many impls. New format = one file, no other layer changed.

```rust
trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn matches(&self, path: &Path) -> bool;
    fn fetch(&self, path: &Path) -> Result<Vec<Entry>>;
    fn list(&self, root: &Path)  -> Result<Vec<Entry>> { /* default: walk + fetch */ }
    fn search(&self, q: &str, root: &Path) -> Result<Vec<Entry>> { Ok(vec![]) }
}

struct Entry {
    id:   String,                       // stable: file path + sha
    kind: String,                       // "row" | "section" | "doc"
    text: String,                       // canonical body the LLM reads
    meta: BTreeMap<String, String>,     // path, mime, sha, ...
}
```

Hackathon set: `JsonProvider`, `CsvProvider`, `MarkdownProvider`, `PdfProvider` (text-only via `pdf-extract`), `DirectoryProvider` (synthetic — children as Entries).

Register at startup. Dispatch by `for_path(path)`.

---

## 4. Knowledge graph

Builder accumulates an in-memory graph. Domain types live in builder; never leak into `core`.

```rust
struct Entity { id: EntityId, kind: String, fields: BTreeMap<String,String>, sources: Vec<SourceRef> }
struct Edge   { from: EntityId, to: EntityId, rel: String, source: SourceRef, conf: u32 }
```

LLM passes (Claude Haiku 4.5, temp 0, prompt-cache the system msg):

1. **Extract** — `Entry → Vec<EntityCandidate>`. Pin entity_type vocabulary in prompt.
2. **Resolve** — alias table + LLM tiebreak (Sonnet 4.6 only when ambiguous). Match candidates to existing entities or mint new.
3. **Relate** — second pass over text + neighborhood, emits `Vec<Edge>` (`owns`, `reports_to`, `assigned_to`, ...).

Snapshot graph to `./.qontext/graph.json` periodically for crash recovery. Not the truth — re-derivable from `source/`.

---

## 5. Render: graph → Nodes

Builder projects `Graph` to paths and writes via `core::state::upsert_batch` (one refresh per change set, so readers see consistent snapshot).

| What           | Path                                | Content                                                  |
|----------------|-------------------------------------|----------------------------------------------------------|
| Entity card    | `/{kind}/{id}/_.md`                 | YAML front-matter + fields + source list                 |
| Edge subdir    | `/{kind}/{id}/{rel}/`               | Dir whose `children` are linked entity paths             |
| Edge leaf      | `/{kind}/{id}/{rel}/{target_id}.md` | Link node; `meta["link_target"]` = absolute target path  |
| Graph dump     | `/_graph/edges.jsonl`               | Audit + agent grep target                                |

Render is deterministic from graph. Re-render only paths whose entities or edges changed.

---

## 6. Shared state — minimal

`core::Node` is intentionally thin. Heavy domain types stay in builder.

```rust
struct Node {
    kind:       NodeKind,                 // File | Dir | Link
    content:    String,                   // text body (markdown for cards, json for dumps, ...)
    children:   Vec<String>,              // absolute paths
    size:       u64,
    mtime_secs: u64,
    etag:       u64,                      // monotonic; WS uses it to detect changes
    meta:       BTreeMap<String,String>,  // builder-defined tags (entity_type, link_target, source_uri, conf, human_notes, ...)
}
```

Stored in evmap (`HashMap<String, Arc<Node>>`-shape, lock-free reads, single writer). Detail in `RUST_PLAN.md §1`.

No `provenance`, `fact_id`, conflict types in core. Builder serializes richer concepts into `meta` keys or sidecar nodes.

---

## 7. Auto-update

`notify` recursive watch on `source/`. Per change:

1. Find matching provider for the path.
2. `provider.fetch(path)` → fresh Entries.
3. Hash-cache by `(path, sha)`. Skip if identical.
4. Re-extract entities for those Entries; merge into graph.
5. Re-render only paths whose entities or edges touched.

Target: <3s from save to mount update. Demo edits a CSV row in front of judges.

---

## 8. Query surface

The mounted tree IS the query API. Anything reading the FS works (Claude Code, ripgrep, fzf, fd). `cd` and `ls` are the navigation.

HTTP mirrors with structured ops:

```
GET  /healthz
GET  /api/tree?prefix=/customers
GET  /api/node/*path                  → { kind, content, children, meta, size, etag, mtime_secs }
GET  /api/query?entity=&id=&edge=     → [Node] of related entities
GET  /api/search?q=...                → grep over Node.content + meta
GET  /api/stats
GET  /api/events                      → WS push of {path, etag} on every refresh
POST /api/source                      → drop a file into ./source/{path}
```

---

## 9. 24h plan

| Phase | h     | Builder                                              | FS                          | API                       |
|-------|-------|------------------------------------------------------|-----------------------------|---------------------------|
| 0     | 0–2   | workspace; trait + JsonProvider stub                 | mount empty FS              | /healthz, /api/tree       |
| 1     | 2–6   | source watch + drain; json+csv+md providers; render Entry → 1:1 mirror Nodes (no LLM) | renders source 1:1 | /api/node/*               |
| 2     | 6–12  | LLM extract → entities; alias resolver; render `/{kind}/{id}/_.md` | unchanged          | /api/query stub           |
| 3     | 12–18 | LLM relate → edges; render edge subdirs              | unchanged                   | /api/query real; WS       |
| 4     | 18–22 | PdfProvider; auto-update; graph snapshot             | polish                      | /api/search; /api/source  |
| 5     | 22–24 | demo data, 3 dry runs, video backup                  |                             |                           |

---

## 10. Demo (5 min)

1. **0:30** Show empty `/mnt/qontext/` and a populated `./source/` with 3 subdirs (`crm/*.json`, `hr/*.csv`, `policies/*.md`).
2. **1:00** Start `qontext`. Tree fills live (left pane), WS log streams (right).
3. **2:00** `cd /mnt/qontext/customers/acme-co/orders && ls` — orders appear.
4. **2:30** `curl /api/query?entity=customer&id=acme-co&edge=owns_projects`.
5. **3:00** Edit a CSV row in `source/hr/`. Within 3s the employee card and its `reports_to/` subdir update.
6. **3:30** Drop a new format file + a 30-line provider impl. Restart builder. New entities appear.
7. **4:00** Open Claude Code on `/mnt/qontext`. Ask "what does Jane own?". Agent reads files, answers with paths.
8. **4:30** Pitch close: "Source of truth is your folder. Output is a graph any tool reads natively."

---

## 11. Cut list (drop in order)

1. PdfProvider (text-only mock)
2. WS push (poll instead)
3. `provider.search()` (grep the rendered tree)
4. Graph snapshot persistence (rebuild on restart)
5. Edge subdir render (flatten to `_.md` listing target paths)

**Do not cut:** Provider trait, source watch, entity resolver, FUSE mount, `/api/query`.

---

## 12. Risks

| Risk                                             | Mitigation                                                                                       |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------|
| LLM nondeterminism breaks graph stability        | temp 0, prompt-cache the system msg, eval set of 20 cases by h12                                 |
| Provider misclassifies a file                    | extension whitelist + content sniff; log unmatched paths                                         |
| Huge `source/` floods events                     | debounce 250ms; hash-cache by content                                                            |
| Component coupling drift                         | CI rule: `qontext-fs` and `qontext-api` must not depend on `qontext-builder`. Only `qontext-core`. |
| FUSE on macOS                                    | Linux box for demo. Other 2 components run anywhere.                                             |
| Old "surgical patch / HUMAN_EDITS_BELOW" assumed | Render is full-replace per path. Human edits live in `meta["human_notes"]`, preserved across re-renders. |

End.
