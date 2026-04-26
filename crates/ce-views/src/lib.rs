use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ce_store::{FactMeta, Store, StoreError};
use minijinja::{context, Environment};
use serde::Serialize;
use thiserror::Error;
use tracing::{debug, warn};

const GEN_START: &str = "<!-- ce:generated:start -->";
const GEN_END: &str = "<!-- ce:generated:end -->";
const MAN_START: &str = "<!-- ce:manual:start -->";
const MAN_END: &str = "<!-- ce:manual:end -->";

const DEFAULT_TPL: &str = r#"# {{ id }}

**Type:** {{ entity_type }}
**Sources:** {{ source_count }}

{% if attrs %}## Attributes

{% for p, vals in attrs %}- **{{ p }}**: {% for v in vals %}{{ v.value }}{% if v.adapter %} _({{ v.adapter }}@{{ "%.2f"|format(v.confidence) }}{% if v.locator %}, {{ v.locator }}{% endif %})_{% endif %}{% if not loop.last %}; {% endif %}{% endfor %}
{% endfor %}{% endif %}
{% if outgoing %}## Outgoing references

{% for p, vals in outgoing %}- **{{ p }}**: {% for v in vals %}[{{ v.value }}]({{ v.link }}){% if v.adapter %} _({{ v.adapter }}@{{ "%.2f"|format(v.confidence) }})_{% endif %}{% if not loop.last %}; {% endif %}{% endfor %}
{% endfor %}{% endif %}
{% if incoming %}## Referenced by

{% for r in incoming %}- [{{ r.subject }}]({{ r.link }}) via `{{ r.predicate }}` _({{ r.adapter }}@{{ "%.2f"|format(r.confidence) }})_
{% endfor %}{% endif %}
{% if aliases %}## Aliases

{% for grp, items in aliases %}- **{{ grp }}**: {% for a in items %}`{{ a.alias }}`{% if a.confidence < 1.0 %} _({{ "%.2f"|format(a.confidence) }})_{% endif %}{% if not loop.last %}, {% endif %}{% endfor %}
{% endfor %}{% endif %}
"#;

#[derive(Debug, Error)]
pub enum ViewError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("template: {0}")]
    Template(#[from] minijinja::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Serialize)]
struct FactView {
    value: String,
    adapter: String,
    confidence: f64,
    locator: Option<String>,
    /// Relative markdown link target. Set only for outgoing reference facts.
    #[serde(skip_serializing_if = "Option::is_none")]
    link: Option<String>,
}

#[derive(Serialize)]
struct IncomingView {
    subject: String,
    predicate: String,
    adapter: String,
    confidence: f64,
    link: String,
}

#[derive(Serialize)]
struct AliasView {
    alias: String,
    confidence: f64,
}

pub struct Renderer {
    env: Environment<'static>,
}

impl Renderer {
    pub fn new() -> Result<Self, ViewError> {
        let mut env = Environment::new();
        env.add_template("default.md", DEFAULT_TPL)?;
        Ok(Self { env })
    }

    pub fn add_template(&mut self, entity_type: &str, source: String) -> Result<(), ViewError> {
        let name: &'static str = Box::leak(format!("{}.md", entity_type).into_boxed_str());
        let leaked: &'static str = Box::leak(source.into_boxed_str());
        self.env.add_template(name, leaked)?;
        Ok(())
    }

    pub fn render_one(
        &self,
        id: &str,
        entity_type: &str,
        facts: &[FactMeta],
    ) -> Result<(String, usize), ViewError> {
        self.render_with_context(id, entity_type, facts, &[], &[])
    }

    /// Backwards-compatible: forwards to `render_with_context` with no aliases.
    pub fn render_with_incoming(
        &self,
        id: &str,
        entity_type: &str,
        facts: &[FactMeta],
        incoming: &[(String, String, String, String, f64)],
    ) -> Result<(String, usize), ViewError> {
        self.render_with_context(id, entity_type, facts, incoming, &[])
    }

    /// Render an entity into markdown: attribute facts, outgoing `ref:*` edges,
    /// incoming references, and the alias bundle. Aliases are grouped by
    /// source (`id`, `field:<predicate>`, `email_local`, `derived`).
    pub fn render_with_context(
        &self,
        id: &str,
        entity_type: &str,
        facts: &[FactMeta],
        incoming: &[(String, String, String, String, f64)],
        aliases: &[(String, String, f64)], // alias, source, confidence
    ) -> Result<(String, usize), ViewError> {
        let mut attrs: BTreeMap<String, Vec<FactView>> = BTreeMap::new();
        let mut outgoing: BTreeMap<String, Vec<FactView>> = BTreeMap::new();
        let mut sources = std::collections::HashSet::new();
        for f in facts {
            let v: serde_json::Value = serde_json::from_str(&f.object_json)
                .unwrap_or(serde_json::Value::String(f.object_json.clone()));
            let value = match &v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            sources.insert(f.adapter.clone());
            if let Some(target_type) = f.predicate.strip_prefix("ref:") {
                let link = format!("../{}/{}.md", target_type, sanitize(&value));
                outgoing.entry(f.predicate.clone()).or_default().push(FactView {
                    value,
                    adapter: f.adapter.clone(),
                    confidence: f.confidence,
                    locator: f.locator.clone(),
                    link: Some(link),
                });
            } else {
                attrs.entry(f.predicate.clone()).or_default().push(FactView {
                    value,
                    adapter: f.adapter.clone(),
                    confidence: f.confidence,
                    locator: f.locator.clone(),
                    link: None,
                });
            }
        }
        let attrs_vec: Vec<(String, Vec<FactView>)> = attrs.into_iter().collect();
        let outgoing_vec: Vec<(String, Vec<FactView>)> = outgoing.into_iter().collect();
        let incoming_vec: Vec<IncomingView> = incoming
            .iter()
            .map(|(predicate, subj_id, subj_type, adapter, conf)| {
                let link = if subj_type.is_empty() {
                    format!("./{}.md", sanitize(subj_id))
                } else {
                    format!("../{}/{}.md", subj_type, sanitize(subj_id))
                };
                IncomingView {
                    subject: subj_id.clone(),
                    predicate: predicate.clone(),
                    adapter: adapter.clone(),
                    confidence: *conf,
                    link,
                }
            })
            .collect();
        let tpl_name = format!("{}.md", entity_type);
        let tpl = self
            .env
            .get_template(&tpl_name)
            .or_else(|_| self.env.get_template("default.md"))?;
        // Group aliases by source for a tidy markdown listing.
        let mut alias_groups: BTreeMap<String, Vec<AliasView>> = BTreeMap::new();
        for (alias, source, conf) in aliases {
            alias_groups.entry(source.clone()).or_default().push(AliasView {
                alias: alias.clone(),
                confidence: *conf,
            });
        }
        let aliases_vec: Vec<(String, Vec<AliasView>)> = alias_groups.into_iter().collect();

        let body = tpl.render(context! {
            id => id,
            entity_type => entity_type,
            source_count => sources.len(),
            attrs => attrs_vec,
            outgoing => outgoing_vec,
            incoming => incoming_vec,
            aliases => aliases_vec,
        })?;
        Ok((body, sources.len()))
    }
}

pub struct BuildStats {
    pub written: usize,
    pub skipped: usize,
}

pub fn build_all(store: &Store, renderer: &Renderer, out: &Path, now: i64) -> Result<BuildStats, ViewError> {
    let mut stats = BuildStats { written: 0, skipped: 0 };
    let entities = store.all_entities()?;
    // Bulk pre-fetch — avoids N+1 for both inverse edges and the alias bundle.
    let incoming_index = store.all_incoming_refs()?;
    let alias_index = store.aliases_grouped_full()?;
    let empty_in: Vec<(String, String, String, String, f64)> = Vec::new();
    let empty_al: Vec<(String, String, f64)> = Vec::new();
    for (id, etype) in entities {
        let facts = store.facts_for_subject(&id)?;
        if facts.is_empty() {
            stats.skipped += 1;
            continue;
        }
        let incoming = incoming_index.get(&id).unwrap_or(&empty_in);
        let aliases = alias_index.get(&id).unwrap_or(&empty_al);
        let (body, source_count) =
            renderer.render_with_context(&id, &etype, &facts, incoming, aliases)?;
        let path = out.join(&etype).join(format!("{}.md", sanitize(&id)));
        write_idempotent(&path, &etype, &id, source_count, now, &body)?;
        stats.written += 1;
    }
    Ok(stats)
}

fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}

fn write_idempotent(
    path: &Path,
    entity_type: &str,
    id: &str,
    source_count: usize,
    now: i64,
    generated: &str,
) -> Result<(), ViewError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let manual = read_manual_block(path).unwrap_or_default();
    let frontmatter = format!(
        "---\nid: {}\ntype: {}\nlast_generated: {}\nsource_count: {}\n---\n",
        id, entity_type, now, source_count
    );
    let content = format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}\n{}\n",
        frontmatter, GEN_START, generated.trim_end(), GEN_END, MAN_START, manual.trim_end(), MAN_END
    );
    if let Ok(existing) = fs::read_to_string(path) {
        if existing == content {
            debug!(path=%path.display(), "unchanged");
            return Ok(());
        }
    }
    fs::write(path, content)?;
    Ok(())
}

fn read_manual_block(path: &Path) -> Option<String> {
    let s = fs::read_to_string(path).ok()?;
    let start = s.find(MAN_START)? + MAN_START.len();
    let end_rel = s[start..].find(MAN_END)?;
    Some(s[start..start + end_rel].trim_matches('\n').to_string())
}

pub fn load_template_dir(renderer: &mut Renderer, dir: &Path) -> Result<usize, ViewError> {
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut n = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) != Some("md") {
            continue;
        }
        let stem = match p.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        match fs::read_to_string(&p) {
            Ok(src) => {
                renderer.add_template(&stem, src)?;
                n += 1;
            }
            Err(e) => warn!(path=%p.display(), error=%e, "template read failed"),
        }
    }
    Ok(n)
}

pub fn out_path(out: &Path, entity_type: &str, id: &str) -> PathBuf {
    out.join(entity_type).join(format!("{}.md", sanitize(id)))
}
