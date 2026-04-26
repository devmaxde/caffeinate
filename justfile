# Context Engine — common dev tasks.
# Run `just` to list, `just <task>` to invoke.

set shell := ["bash", "-cu"]
set dotenv-load := true

# Defaults — override on the command line: `just store=foo.db ingest ./data`
store    := "ce.sqlite"
idx      := "ce.idx"
out      := "out"
data     := "./EnterpriseBench"
addr     := "0.0.0.0:3000"
ce       := "cargo run --release --quiet -p ce-cli --"




default:
    @just --list

# --- build / quality ---------------------------------------------------------

build:
    cargo build --release

check:
    cargo check --workspace --all-targets

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

test:
    cargo test --workspace

clean-db:
    rm -rf {{store}} {{idx}} {{out}}

cli +args:
    cargo run --release --quiet -p ce-cli -- {{args}}


# --- ingest ------------------------------------------------------------------

# Structured pass over a folder. Override: `just data=./mydir ingest`
ingest folder=data:
    {{ce}} ingest {{folder}} --store {{store}}

# Discover schemas without writing.
ingest-dry folder=data:
    {{ce}} ingest {{folder}} --store {{store}} --dry-run

# Structured + LLM pass. `just ingest-llm 100` caps at 100 sections.
# Provider/model/concurrency come from ce.toml. Override with env or --llm-* flags via `just cli`.
ingest-llm n="100" folder=data:
    {{ce}} ingest {{folder}} --store {{store}} --max-llm {{n}}

# --- conflicts ---------------------------------------------------------------

conflicts:
    {{ce}} inspect conflicts --store {{store}}

resolve:
    {{ce}} resolve --store {{store}}

# --- views / index -----------------------------------------------------------

build-views:
    {{ce}} build --store {{store}} --out {{out}}

build-views-tpl tpl:
    {{ce}} build --store {{store}} --out {{out}} --templates {{tpl}}

index:
    {{ce}} index --store {{store}} --idx {{idx}}

# --- search / aggregates -----------------------------------------------------

search query k="10" hops="0":
    {{ce}} search "{{query}}" --store {{store}} --idx {{idx}} --k {{k}} --hops {{hops}}

agg-counts:
    {{ce}} agg entity-counts --store {{store}}

agg-preds type="":
    {{ce}} agg predicate-counts {{ if type != "" { "--type " + type } else { "" } }} --store {{store}}

agg-top pred limit="20":
    {{ce}} agg top-values {{pred}} --limit {{limit}} --store {{store}}

# --- HTTP API ----------------------------------------------------------------

serve:
    {{ce}} serve --store {{store}} --idx {{idx}} --addr {{addr}}

serve-no-idx:
    {{ce}} serve --store {{store}} --addr {{addr}}

# Quick API smoke tests (server must be running).
api-health:
    curl -s http://{{addr}}/health && echo

api-entity etype id:
    curl -s http://{{addr}}/entities/{{etype}}/{{id}} | jq .

api-retrieve query k="10" hops="0":
    curl -s -X POST http://{{addr}}/retrieve \
        -H 'content-type: application/json' \
        -d '{"query":"{{query}}","k":{{k}},"hops":{{hops}}}' | jq .

# --- end-to-end --------------------------------------------------------------

# Full pipeline: ingest → resolve → views → index. Then `just serve`.
all folder=data:
    just ingest {{folder}}
    just resolve
    just build-views
    just index
