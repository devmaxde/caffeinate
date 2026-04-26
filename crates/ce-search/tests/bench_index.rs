//! Indexing benchmark. Run with:
//!     cargo test -p ce-search --release --test bench_index -- --nocapture
//! Tune via env: CE_BENCH_ENTITIES, CE_BENCH_FACTS_PER, CE_BENCH_TYPES.

use std::path::PathBuf;
use std::time::Instant;

use ce_core::{Entity, Fact, Provenance, SourceRef};
use ce_search::{build_graph, build_index, search};
use ce_store::Store;
use tempfile::tempdir;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[test]
fn bench_build_index() {
    // Real-data mode: point at an existing sqlite. Skips synthetic populate.
    if let Ok(real) = std::env::var("CE_BENCH_STORE") {
        bench_real(PathBuf::from(real));
        return;
    }

    let n_entities = env_usize("CE_BENCH_ENTITIES", 20_000);
    let facts_per = env_usize("CE_BENCH_FACTS_PER", 12);
    let n_types = env_usize("CE_BENCH_TYPES", 20);

    let tmp = tempdir().unwrap();
    let store_path: PathBuf = tmp.path().join("bench.sqlite");
    let idx_path: PathBuf = tmp.path().join("bench.idx");

    eprintln!(
        "== bench: entities={} facts/entity={} types={} ==",
        n_entities, facts_per, n_types
    );

    // -- populate ----
    let t0 = Instant::now();
    let store = Store::open(&store_path).unwrap();
    // wrap inserts in a single transaction → orders of magnitude faster.
    store.conn.execute_batch("BEGIN").unwrap();
    let now = 1_700_000_000i64;
    let src = SourceRef {
        path: PathBuf::from("synthetic"),
        byte_range: None,
        locator: None,
    };
    for i in 0..n_entities {
        let etype = format!("type_{}", i % n_types);
        let id = format!("e{:08}", i);
        store
            .upsert_entity(&Entity { id: id.clone(), entity_type: etype.clone() })
            .unwrap();
        for j in 0..facts_per {
            let pred = if j == 0 && i > 0 {
                "ref:owner".to_string()
            } else {
                format!("attr_{}", j)
            };
            let obj = if pred.starts_with("ref:") {
                serde_json::json!(format!("e{:08}", i - 1))
            } else {
                serde_json::json!(format!("value {} for entity {}", j, i))
            };
            let fact = Fact {
                subject: id.clone(),
                predicate: pred,
                object: obj,
                provenance: Provenance {
                    source: src.clone(),
                    adapter: "synthetic".into(),
                    confidence: 1.0,
                    observed_at: now,
                },
            };
            store.insert_fact(&fact, None).unwrap();
        }
    }
    store.conn.execute_batch("COMMIT").unwrap();
    eprintln!("populate:     {:>8.2?}", t0.elapsed());

    // -- index ----
    let t1 = Instant::now();
    let n = build_index(&store, &idx_path).unwrap();
    let dt_idx = t1.elapsed();
    let throughput = n as f64 / dt_idx.as_secs_f64();
    eprintln!(
        "build_index:  {:>8.2?}   docs={}   {:>8.0} docs/s",
        dt_idx, n, throughput
    );
    assert_eq!(n, n_entities);

    // -- graph ----
    let t2 = Instant::now();
    let g = build_graph(&store).unwrap();
    eprintln!(
        "build_graph:  {:>8.2?}   nodes={}  edges={}",
        t2.elapsed(),
        g.ix_to_id.len(),
        g.g.edge_count()
    );

    // -- query smoke ----
    let t3 = Instant::now();
    let hits = search(&idx_path, "value 3 for entity", 10).unwrap();
    eprintln!("search(k=10): {:>8.2?}   hits={}", t3.elapsed(), hits.len());
    assert!(!hits.is_empty());
}

fn bench_real(store_path: PathBuf) {
    assert!(store_path.exists(), "CE_BENCH_STORE not found: {}", store_path.display());
    let tmp = tempdir().unwrap();
    let idx_path = tmp.path().join("real.idx");

    eprintln!("== real-data bench: store={} ==", store_path.display());

    let t_open = Instant::now();
    let store = Store::open(&store_path).unwrap();
    eprintln!("open store:   {:>8.2?}", t_open.elapsed());

    let t_count = Instant::now();
    let n_entities: i64 = store
        .conn
        .query_row("SELECT count(*) FROM entities", [], |r| r.get(0))
        .unwrap();
    let n_facts: i64 = store
        .conn
        .query_row("SELECT count(*) FROM facts", [], |r| r.get(0))
        .unwrap();
    eprintln!(
        "stats:        {:>8.2?}   entities={}  facts={}",
        t_count.elapsed(),
        n_entities,
        n_facts
    );

    let t1 = Instant::now();
    let n = build_index(&store, &idx_path).unwrap();
    let dt = t1.elapsed();
    eprintln!(
        "build_index:  {:>8.2?}   docs={}   {:>8.0} docs/s",
        dt,
        n,
        n as f64 / dt.as_secs_f64()
    );

    let t2 = Instant::now();
    let g = build_graph(&store).unwrap();
    eprintln!(
        "build_graph:  {:>8.2?}   nodes={}  edges={}",
        t2.elapsed(),
        g.ix_to_id.len(),
        g.g.edge_count()
    );

    let t3 = Instant::now();
    let hits = search(&idx_path, "the", 10).unwrap();
    eprintln!("search(k=10): {:>8.2?}   hits={}", t3.elapsed(), hits.len());
}
