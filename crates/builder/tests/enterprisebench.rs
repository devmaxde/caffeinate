//! Reads EnterpriseBench/. Builds:
//!   1. file-mirror node graph (evmap)
//!   2. per-file entity groups (Vec<type>)
//!   3. heuristic + LLM cross-ref edges → adjacency graph (Arc<Mutex>)
//!
//! - Truncated file tree → stderr.
//! - Full file tree → target/graph.txt
//! - Group + adjacency dump → stderr + target/entities.txt
//! - LLM step skipped unless ANTHROPIC_API_KEY is set.

use qontext_builder::{maxi, relate, EntityGraph, MaxiGraph};
use rayon::prelude::*;
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

    // ---- maxi-graph ----
    let t0 = std::time::Instant::now();
    let maxi_graph = MaxiGraph::from_legacy(res.groups, &edges);
    let build_ms = t0.elapsed().as_millis();
    let total_entries: usize = maxi_graph
        .store
        .groups
        .iter()
        .map(|g| g.entries.len())
        .sum();
    eprintln!(
        "\n=== maxi-graph built in {}ms: {} groups, {} entries indexed ===",
        build_ms,
        maxi_graph.store.groups.len(),
        total_entries
    );

    let t0 = std::time::Instant::now();
    let md_map = maxi::render_all_md(&maxi_graph);
    let render_ms = t0.elapsed().as_millis();
    eprintln!(
        "rendered {} markdown views in {}ms (rayon)",
        md_map.len(),
        render_ms
    );
    assert_eq!(md_map.len(), total_entries);

    let cust_path = "/Customer_Relation_Management/customers.json";
    let cust_idx = maxi_graph.group_idx(cust_path).expect("customers group");
    let sample_md = maxi::render_entry_md(&maxi_graph, cust_idx, 0);
    eprintln!(
        "\n=== sample maxi md: customers / {} ===\n{}",
        maxi_graph.store.groups[cust_idx].entries[0].id, sample_md
    );

    let maxi_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/maxi");
    std::fs::create_dir_all(&maxi_dir).ok();
    let dumps = [
        ("/Customer_Relation_Management/customers.json", 3),
        ("/Customer_Relation_Management/products.json", 2),
        ("/Human_Resource_Management/Employees/employees.json", 2),
    ];
    let mut written = 0usize;
    for (path, n) in dumps {
        if let Some(gi) = maxi_graph.group_idx(path) {
            let g = &maxi_graph.store.groups[gi];
            let take = n.min(g.entries.len());
            for ei in 0..take {
                let id = &g.entries[ei].id;
                let md = maxi::render_entry_md(&maxi_graph, gi, ei);
                let p = maxi_dir.join(format!("{}__{}.md", g.name, id));
                std::fs::write(&p, &md).ok();
                written += 1;
            }
        }
    }
    eprintln!(
        "\n=== {} sample maxi mds written to {} ===",
        written,
        maxi_dir.display()
    );
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

/// Independent test: scan EnterpriseBench, build maxi-graph (heuristic edges),
/// dump every entry as Markdown to `target/maxi/<group>/<idx>_<id>.md` in parallel.
/// Skips evmap state, so it can run alongside the other test in the same binary.
#[test]
fn dump_full_maxi_to_target() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../EnterpriseBench")
        .canonicalize()
        .expect("EnterpriseBench dir must exist");

    let t0 = std::time::Instant::now();
    let res = qontext_builder::scan(&root).expect("scan");
    eprintln!(
        "[dump] scan: {} groups, {} nodes in {}ms",
        res.groups.len(),
        res.nodes.len(),
        t0.elapsed().as_millis()
    );

    let edges = relate::heuristic_edges(&res.groups);
    eprintln!("[dump] {} heuristic edges", edges.len());

    let t0 = std::time::Instant::now();
    let graph = MaxiGraph::from_legacy(res.groups, &edges);
    let total: usize = graph
        .store
        .groups
        .iter()
        .map(|g| g.entries.len())
        .sum();
    eprintln!(
        "[dump] maxi built in {}ms ({} entries)",
        t0.elapsed().as_millis(),
        total
    );

    let out_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/maxi");
    let _ = std::fs::remove_dir_all(&out_root);
    std::fs::create_dir_all(&out_root).expect("mkdir target/maxi");
    for g in &graph.store.groups {
        std::fs::create_dir_all(out_root.join(&g.name)).expect("mkdir group");
    }

    let pairs: Vec<(usize, usize)> = graph
        .store
        .groups
        .iter()
        .enumerate()
        .flat_map(|(gi, g)| (0..g.entries.len()).map(move |ei| (gi, ei)))
        .collect();

    let t0 = std::time::Instant::now();
    pairs.par_iter().for_each(|&(gi, ei)| {
        let g = &graph.store.groups[gi];
        let id = sanitize(&g.entries[ei].id);
        let path = out_root
            .join(&g.name)
            .join(format!("{:08}_{}.md", ei, id));
        let md = qontext_builder::render_entry_md(&graph, gi, ei);
        std::fs::write(&path, md).expect("write md");
    });
    let ms = t0.elapsed().as_millis();
    eprintln!(
        "[dump] wrote {} markdown files to {} in {}ms",
        pairs.len(),
        out_root.display(),
        ms
    );

    for g in &graph.store.groups {
        let dir = out_root.join(&g.name);
        let count = std::fs::read_dir(&dir)
            .map(|it| it.count())
            .unwrap_or(0);
        eprintln!("  [{}] {} files in {}", g.provider, count, dir.display());
        assert_eq!(
            count,
            g.entries.len(),
            "file count mismatch for group {}",
            g.name
        );
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
