# Qontext

Watch a folder of company data. Per-format providers extract structure. AI builds a dynamic knowledge graph. Output is a virtual filesystem where every entity is queryable — `customers/acme/orders/` returns the orders.

Full design:
- `QONTEXT_PLAN.md` — product
- `RUST_PLAN.md` — engine

## Workspace layout

```
crates/
├── core/      shared lib: state (evmap), Node model, ids
├── fs/        FUSE3 daemon binary (read-only mount of core::state)
├── builder/   source/ watcher + providers + LLM extract/resolve/relate + render
├── api/       axum HTTP + WS over core::state
└── qontext/   orchestrator: spawns all 3 in one process, one shared evmap
```

**Independence rule:** every binary depends on `qontext-core` and nothing else cross-crate. `qontext-fs` and `qontext-api` must not depend on `qontext-builder`. Each binary is **standalone runnable** — inits its own state, optionally seeds fixtures, runs its loop.

## Surface

- `./source/` — on-disk source of truth. Subfolders by domain (`crm/`, `hr/`, `policies/`, ...). Files in any format the providers know.
- `/mnt/qontext/` — virtual, in-memory, FUSE-served output. Per-entity tree:

```
/customers/acme-co/_.md
/customers/acme-co/orders/ord-441.md
/employees/jane-smith/_.md
/employees/jane-smith/owns_projects/p-12.md
/_graph/edges.jsonl
```

The HTTP API exposes the same tree as JSON plus structured queries (`/api/query?entity=customer&id=acme-co&edge=orders`).

## Running

### Solo (per-component dev)

```bash
cargo run -p qontext-fs        # mount only (Linux). Seeded demo data.
cargo run -p qontext-builder   # watch ./source/, run providers, write Nodes
cargo run -p qontext-api       # HTTP on :8080. Seeded demo data.
```

### Integrated

```bash
cargo run -p qontext           # state::init() once, builder + fs + api share one evmap
```

## Environment

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export QONTEXT_SOURCE=./source           # default
export QONTEXT_MOUNT=/tmp/qontext        # default
export QONTEXT_ADDR=0.0.0.0:8080         # default
export RUST_LOG=qontext=debug,fuser=warn
```

## Hour-0 checklist

- [ ] Demo box: Linux confirmed (FUSE3 only works on Linux without macFUSE pain)
- [ ] `ANTHROPIC_API_KEY` exported
- [ ] `cargo check --workspace` passes
- [ ] Branches created, ownership agreed
- [ ] `./source/` skeleton with sample json/csv/md files
