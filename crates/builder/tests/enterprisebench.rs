//! Reads EnterpriseBench/. Builds:
//!   1. file-mirror node graph (evmap)
//!   2. per-file entity groups (Vec<type>)
//!   3. heuristic + LLM cross-ref edges → adjacency graph (Arc<Mutex>)
//!
//! - Truncated file tree → stderr.
//! - Full file tree → target/graph.txt
//! - Group + adjacency dump → stderr + target/entities.txt
//! - LLM step skipped unless ANTHROPIC_API_KEY is set.

use qontext_builder::{relate, EntityGraph};
use qontext_core::model::{FileNode, NodeKind};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;

const TRUNC_DEPTH: usize = 3;
const TRUNC_KIDS: usize = 6;

#[tokio::test(flavor = "multi_thread")]
async fn index_enterprisebench_into_graph() {
    qontext_core::state::init();

    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../EnterpriseBench")
        .canonicalize()
        .expect("EnterpriseBench dir must exist next to crates/");

    let res = qontext_builder::index(&root).expect("index ok");
    assert!(!res.nodes.is_empty(), "expected nodes, got 0");
    assert!(!res.groups.is_empty(), "expected groups, got 0");

    let root_node = qontext_core::state::read_node("/").expect("/ exists");
    assert!(root_node.is_dir());
    assert!(!root_node.children.is_empty());

    eprintln!(
        "\n=== file graph: {} nodes, root has {} children ===\n",
        qontext_core::state::len(),
        root_node.children.len()
    );
    let mut tree = String::new();
    print_tree("/", 0, &mut tree, true);
    eprint!("{}", tree);

    let dump_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/graph.txt");
    let mut f = std::fs::File::create(&dump_path).expect("open dump");
    let mut full = String::new();
    print_tree("/", 0, &mut full, false);
    f.write_all(full.as_bytes()).expect("write dump");
    eprintln!("\n=== full file tree dumped to {} ===\n", dump_path.display());

    eprintln!("=== entity groups: {} ===", res.groups.len());
    for g in &res.groups {
        eprintln!(
            "  [{}] {} ({} entries, {} keys)\n      path: {}\n      keys: {}",
            g.provider,
            g.name,
            g.entries.len(),
            g.keys.len(),
            g.source_path,
            short_list(g.keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), 8)
        );
    }

    let edges = if std::env::var("OPENROUTER_API_KEY").is_ok() {
        let model = std::env::var("QONTEXT_LLM_MODEL")
            .unwrap_or_else(|_| relate::DEFAULT_MODEL.to_string());
        eprintln!("\n=== calling LLM via rig openrouter (model={}) ===", model);
        match relate::infer_edges_llm(&res.groups).await {
            Ok(e) => {
                eprintln!("LLM returned {} edges", e.len());
                e
            }
            Err(err) => {
                eprintln!("LLM call failed ({}); falling back to heuristic", err);
                relate::heuristic_edges(&res.groups)
            }
        }
    } else {
        eprintln!("\n=== OPENROUTER_API_KEY not set: using heuristic edges ===");
        relate::heuristic_edges(&res.groups)
    };

    eprintln!("=== {} edges ===", edges.len());
    for e in &edges {
        eprintln!(
            "  {}  <->  {}    ({} ↔ {})  // {}",
            short_path(&e.a),
            short_path(&e.b),
            e.a_key.as_deref().unwrap_or("·"),
            e.b_key.as_deref().unwrap_or("·"),
            e.reason
        );
    }

    let graph = EntityGraph::build(&res.groups, &edges);
    assert_eq!(graph.len(), res.groups.len());

    let ent_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/entities.txt");
    let mut ef = std::fs::File::create(&ent_path).expect("open entities dump");
    eprintln!("\n=== entity adjacency ===");
    let mut adj_dump = String::new();
    for g in &res.groups {
        let neigh = graph.neighbors_of(&g.source_path);
        let line = format!(
            "{} ({} entries) -> {} neighbours\n",
            g.name,
            g.entries.len(),
            neigh.len()
        );
        adj_dump.push_str(&line);
        eprintln!(
            "  {} ({} entries) -> {} neighbours",
            g.name,
            g.entries.len(),
            neigh.len()
        );
        for (n_name, n_path, reason) in &neigh {
            let l = format!("    -> {}  [{}]  // {}\n", n_name, n_path, reason);
            adj_dump.push_str(&l);
            eprintln!("    -> {}  // {}", n_name, reason);
        }
    }
    ef.write_all(adj_dump.as_bytes()).expect("write entities");
    eprintln!("\n=== entity adjacency dumped to {} ===", ent_path.display());
}

fn short_path(p: &str) -> String {
    let parts: Vec<&str> = p.rsplitn(3, '/').collect();
    if parts.len() >= 2 {
        format!(".../{}", parts[0])
    } else {
        p.to_string()
    }
}

fn short_list(items: Vec<&str>, n: usize) -> String {
    let head: Vec<_> = items.iter().take(n).copied().collect();
    let mut s = head.join(", ");
    if items.len() > n {
        let _ = write!(s, ", … +{}", items.len() - n);
    }
    s
}

fn print_tree(path: &str, depth: usize, out: &mut String, truncate: bool) {
    let Some(node) = qontext_core::state::read_node(path) else {
        return;
    };
    let indent = "  ".repeat(depth);
    let label = path
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("/");
    let _ = writeln!(out, "{}{} {}{}", indent, glyph(&node), label, summary(&node));

    if !node.is_dir() {
        return;
    }
    if truncate && depth >= TRUNC_DEPTH {
        if !node.children.is_empty() {
            let _ = writeln!(out, "{}  … {} more", indent, node.children.len());
        }
        return;
    }
    let limit = if truncate {
        TRUNC_KIDS.min(node.children.len())
    } else {
        node.children.len()
    };
    for child in &node.children[..limit] {
        print_tree(child, depth + 1, out, truncate);
    }
    if truncate && node.children.len() > limit {
        let _ = writeln!(
            out,
            "{}  … {} more",
            indent,
            node.children.len() - limit
        );
    }
}

fn glyph(n: &FileNode) -> &'static str {
    match n.kind {
        NodeKind::Dir => "[dir]",
        NodeKind::File => "[file]",
        NodeKind::Link => "[link]",
    }
}

fn summary(n: &FileNode) -> String {
    let mut s = String::new();
    if n.is_dir() {
        let _ = write!(s, "  ({} children)", n.children.len());
    } else {
        let _ = write!(s, "  ({} bytes", n.size);
        if let Some(p) = n.meta.get("provider") {
            let _ = write!(s, ", provider={}", p);
        }
        s.push(')');
    }
    s
}
