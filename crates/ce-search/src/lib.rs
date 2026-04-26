use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use ce_store::{Store, StoreError};
use petgraph::graphmap::DiGraphMap;
use rayon::prelude::*;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, STORED, STRING, TEXT};
use tantivy::{doc, Index, IndexWriter, TantivyDocument};
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query: {0}")]
    Query(#[from] tantivy::query::QueryParserError),
    #[error("open: {0}")]
    OpenDir(#[from] tantivy::directory::error::OpenDirectoryError),
}

fn schema() -> (Schema, Field, Field, Field) {
    let mut sb = Schema::builder();
    let id = sb.add_text_field("id", STRING | STORED);
    let entity_type = sb.add_text_field("entity_type", STRING | STORED);
    let text = sb.add_text_field("text", TEXT | STORED);
    (sb.build(), id, entity_type, text)
}

/// Build BM25 index. Parallel: bulk-load facts (single SQL query),
/// build per-entity text payloads with rayon, add docs concurrently
/// via multi-threaded tantivy writer.
pub fn build_index(store: &Store, dir: &Path) -> Result<usize, SearchError> {
    std::fs::create_dir_all(dir)?;
    let (schema, f_id, f_type, f_text) = schema();
    let index = Index::create_in_dir(dir, schema)?;

    // Multi-threaded writer. Cap threads to tantivy's max (8) and
    // give generous heap per thread.
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let writer_threads = cores.clamp(1, 8);
    let mem_per_thread: usize = 256 * 1024 * 1024; // 256 MB
    let writer: IndexWriter =
        index.writer_with_num_threads(writer_threads, mem_per_thread * writer_threads)?;

    // Pull entities + all facts in two queries instead of N+1.
    let entities = store.all_entities()?;
    let facts_flat = store.all_facts_min()?;

    // Group facts by subject.
    let mut facts_by_subj: HashMap<String, Vec<(String, String)>> =
        HashMap::with_capacity(entities.len());
    for (subj, pred, obj) in facts_flat {
        facts_by_subj
            .entry(subj)
            .or_default()
            .push((pred, obj));
    }

    let counter = AtomicUsize::new(0);

    // Parallel build + add. tantivy IndexWriter::add_document takes &self
    // and is safe to call concurrently.
    entities
        .par_iter()
        .try_for_each(|(id, etype)| -> Result<(), SearchError> {
            let Some(facts) = facts_by_subj.get(id) else {
                return Ok(());
            };
            if facts.is_empty() {
                return Ok(());
            }
            let mut buf = String::with_capacity(64 + facts.len() * 32);
            buf.push_str(id);
            buf.push(' ');
            buf.push_str(etype);
            buf.push(' ');
            for (pred, obj) in facts {
                buf.push_str(pred);
                buf.push(' ');
                let v: serde_json::Value =
                    serde_json::from_str(obj).unwrap_or(serde_json::Value::Null);
                match v {
                    serde_json::Value::String(s) => buf.push_str(&s),
                    serde_json::Value::Null => buf.push_str(obj),
                    other => buf.push_str(&other.to_string()),
                }
                buf.push(' ');
            }
            writer.add_document(doc!(
                f_id => id.clone(),
                f_type => etype.clone(),
                f_text => buf,
            ))?;
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;

    // commit needs &mut — re-bind.
    let mut writer = writer;
    writer.commit()?;
    let n = counter.load(Ordering::Relaxed);
    info!(docs = n, dir=%dir.display(), threads=writer_threads, "indexed");
    Ok(n)
}

pub struct SearchHit {
    pub id: String,
    pub entity_type: String,
    pub score: f32,
}

pub fn search(dir: &Path, q: &str, k: usize) -> Result<Vec<SearchHit>, SearchError> {
    let index = Index::open_in_dir(dir)?;
    let f_id = index.schema().get_field("id")?;
    let f_type = index.schema().get_field("entity_type")?;
    let f_text = index.schema().get_field("text")?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let qp = QueryParser::for_index(&index, vec![f_text]);
    let query = qp.parse_query(q)?;
    let top = searcher.search(&query, &TopDocs::with_limit(k))?;
    let mut out = Vec::with_capacity(top.len());
    for (score, addr) in top {
        let d: TantivyDocument = searcher.doc(addr)?;
        let id = first_text(&d, f_id).unwrap_or_default();
        let entity_type = first_text(&d, f_type).unwrap_or_default();
        out.push(SearchHit { id, entity_type, score });
    }
    Ok(out)
}

fn first_text(d: &TantivyDocument, field: Field) -> Option<String> {
    use tantivy::schema::Value;
    d.get_first(field).and_then(|v| v.as_str().map(|s| s.to_string()))
}

pub struct Graph {
    pub g: DiGraphMap<u32, ()>,
    pub id_to_ix: HashMap<String, u32>,
    pub ix_to_id: Vec<String>,
    pub types: HashMap<String, String>,
}

pub fn build_graph(store: &Store) -> Result<Graph, SearchError> {
    let entities = store.all_entities()?;
    let mut id_to_ix: HashMap<String, u32> = HashMap::new();
    let mut ix_to_id: Vec<String> = Vec::new();
    let mut types: HashMap<String, String> = HashMap::new();
    for (id, etype) in entities {
        let ix = ix_to_id.len() as u32;
        id_to_ix.insert(id.clone(), ix);
        types.insert(id.clone(), etype);
        ix_to_id.push(id);
    }

    // Same N+1 fix here: one query, group in mem.
    let facts_flat = store.all_facts_min()?;
    let mut g: DiGraphMap<u32, ()> = DiGraphMap::new();
    for ix in 0..ix_to_id.len() as u32 {
        g.add_node(ix);
    }
    for (subj, pred, obj) in facts_flat {
        if !pred.starts_with("ref:") {
            continue;
        }
        let Some(&from) = id_to_ix.get(&subj) else { continue };
        let target = match serde_json::from_str::<serde_json::Value>(&obj) {
            Ok(serde_json::Value::String(s)) => s,
            _ => continue,
        };
        if let Some(&to) = id_to_ix.get(&target) {
            g.add_edge(from, to, ());
        }
    }
    Ok(Graph { g, id_to_ix, ix_to_id, types })
}

pub fn expand(graph: &Graph, seeds: &[String], hops: usize) -> Vec<(String, String, usize)> {
    let mut visited: HashMap<u32, usize> = HashMap::new();
    let mut q: VecDeque<(u32, usize)> = VecDeque::new();
    for s in seeds {
        if let Some(&ix) = graph.id_to_ix.get(s) {
            visited.insert(ix, 0);
            q.push_back((ix, 0));
        }
    }
    while let Some((ix, d)) = q.pop_front() {
        if d >= hops {
            continue;
        }
        let neighbors: Vec<u32> = graph
            .g
            .neighbors_directed(ix, petgraph::Direction::Outgoing)
            .chain(graph.g.neighbors_directed(ix, petgraph::Direction::Incoming))
            .collect();
        for nb in neighbors {
            if !visited.contains_key(&nb) {
                visited.insert(nb, d + 1);
                q.push_back((nb, d + 1));
            }
        }
    }
    let mut out: Vec<(String, String, usize)> = visited
        .into_iter()
        .map(|(ix, d)| {
            let id = graph.ix_to_id[ix as usize].clone();
            let t = graph.types.get(&id).cloned().unwrap_or_default();
            (id, t, d)
        })
        .collect();
    out.sort_by_key(|(_, _, d)| *d);
    out
}
