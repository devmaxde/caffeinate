use std::path::Path;

use ce_core::{Entity, Fact};
use rusqlite::{params, Connection};
use thiserror::Error;

pub const SCHEMA_SQL: &str = include_str!("../migrations/0001_init.sql");
pub const SCHEMA_V2_SQL: &str = include_str!("../migrations/0002_aliases_embeddings.sql");

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

pub struct Store {
    pub conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        // Speed pragmas: WAL + relaxed sync + in-mem temp + 1 GB mmap.
        // Safe for our workload (ingest is restartable; we don't need
        // crash-durability per-row).
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA temp_store=MEMORY;
             PRAGMA cache_size=-200000;
             PRAGMA mmap_size=1073741824;",
        )?;
        conn.execute_batch(SCHEMA_SQL)?;
        conn.execute_batch(SCHEMA_V2_SQL)?;
        let me = Self { conn };
        me.runtime_migrate()?;
        Ok(me)
    }

    /// Tolerate ALTERs that may have already been applied. Used to retrofit
    /// columns onto stores created by earlier code paths.
    fn runtime_migrate(&self) -> Result<(), StoreError> {
        self.try_alter("ALTER TABLE entity_embeddings ADD COLUMN card_hash TEXT NOT NULL DEFAULT ''")?;
        Ok(())
    }

    fn try_alter(&self, sql: &str) -> Result<(), StoreError> {
        match self.conn.execute(sql, []) {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("duplicate column") { Ok(()) } else { Err(e.into()) }
            }
        }
    }

    /// Wrap a closure in a single transaction. Massive speedup for bulk
    /// inserts vs. autocommit (one fsync per stmt).
    pub fn transaction<F, T>(&mut self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, StoreError>,
    {
        let tx = self.conn.transaction()?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA_SQL)?;
        conn.execute_batch(SCHEMA_V2_SQL)?;
        let me = Self { conn };
        me.runtime_migrate()?;
        Ok(me)
    }

    /// Returns (document_id, status) for the path. Status is one of:
    /// `Cached` — same path + same hash already ingested, no fact work needed.
    /// `Stale` — same path with a different hash exists; old facts have been deleted; caller must re-emit.
    /// `Fresh` — first time we see this path.
    pub fn upsert_document_with_delta(
        &self,
        path: &str,
        adapter: &str,
        content_hash: &str,
        ingested_at: i64,
    ) -> Result<(i64, DeltaStatus), StoreError> {
        // Lookup existing rows by path.
        let prior: Vec<(i64, String)> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id, content_hash FROM documents WHERE path=?")?;
            let rows = stmt
                .query_map(params![path], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                })?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };

        if let Some((id, _)) = prior.iter().find(|(_, h)| h == content_hash) {
            return Ok((*id, DeltaStatus::Cached));
        }

        let status = if prior.is_empty() {
            DeltaStatus::Fresh
        } else {
            // Drop stale documents and any facts that referenced them.
            // Order matters: drop conflicts that point at the to-be-deleted facts first,
            // otherwise the FK on conflicts.fact_a_id/fact_b_id rejects the fact delete.
            let mut del_conflicts = self.conn.prepare(
                "DELETE FROM conflicts WHERE fact_a_id IN (SELECT id FROM facts WHERE document_id=?1)
                                            OR fact_b_id IN (SELECT id FROM facts WHERE document_id=?1)",
            )?;
            let mut del_facts = self
                .conn
                .prepare("DELETE FROM facts WHERE document_id=?")?;
            let mut del_doc = self
                .conn
                .prepare("DELETE FROM documents WHERE id=?")?;
            for (old_id, _) in &prior {
                del_conflicts.execute(params![old_id])?;
                del_facts.execute(params![old_id])?;
                del_doc.execute(params![old_id])?;
            }
            DeltaStatus::Stale
        };

        let id = self.insert_document(path, adapter, content_hash, ingested_at)?;
        Ok((id, status))
    }

    pub fn insert_document(
        &self,
        path: &str,
        adapter: &str,
        content_hash: &str,
        ingested_at: i64,
    ) -> Result<i64, StoreError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO documents(path, adapter, content_hash, ingested_at) VALUES(?,?,?,?)",
            params![path, adapter, content_hash, ingested_at],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT id FROM documents WHERE path=? AND content_hash=?",
            params![path, content_hash],
            |r| r.get(0),
        )?;
        Ok(id)
    }

    pub fn upsert_entity(&self, e: &Entity) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO entities(id, entity_type) VALUES(?,?)",
            params![e.id, e.entity_type],
        )?;
        Ok(())
    }

    pub fn section_cached(&self, hash: &str) -> Result<bool, StoreError> {
        let n: i64 = self.conn.query_row(
            "SELECT count(*) FROM section_cache WHERE section_hash=?",
            params![hash],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    pub fn mark_section(&self, hash: &str, fact_count: usize, when: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO section_cache(section_hash, extracted_at, fact_count) VALUES(?,?,?)",
            params![hash, when, fact_count as i64],
        )?;
        Ok(())
    }

    pub fn entities_by_type(&self) -> Result<std::collections::HashMap<String, Vec<String>>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT entity_type, id FROM entities")?;
        let mut rows = stmt.query([])?;
        let mut out: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        while let Some(r) = rows.next()? {
            let t: String = r.get(0)?;
            let id: String = r.get(1)?;
            out.entry(t).or_default().push(id);
        }
        Ok(out)
    }

    pub fn insert_fact(&self, f: &Fact, document_id: Option<i64>) -> Result<i64, StoreError> {
        let object_json = serde_json::to_string(&f.object)?;
        let locator = f.provenance.source.locator.clone();
        self.conn.execute(
            "INSERT INTO facts(subject, predicate, object_json, document_id, adapter, confidence, observed_at, locator)
             VALUES(?,?,?,?,?,?,?,?)",
            params![
                f.subject,
                f.predicate,
                object_json,
                document_id,
                f.provenance.adapter,
                f.provenance.confidence as f64,
                f.provenance.observed_at,
                locator,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Bulk path: insert many (entity, facts) pairs in one transaction.
    /// Skips per-fact conflict detection — call `detect_conflicts_bulk`
    /// once at the end to populate the conflicts table.
    pub fn bulk_ingest(
        &mut self,
        entities: &[Entity],
        facts: &[(Fact, Option<i64>)],
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        {
            let mut up_ent = tx.prepare(
                "INSERT OR IGNORE INTO entities(id, entity_type) VALUES(?,?)",
            )?;
            for e in entities {
                up_ent.execute(params![e.id, e.entity_type])?;
            }
            let mut ins_fact = tx.prepare(
                "INSERT INTO facts(subject, predicate, object_json, document_id, adapter, confidence, observed_at, locator)
                 VALUES(?,?,?,?,?,?,?,?)",
            )?;
            for (f, doc_id) in facts {
                let object_json = serde_json::to_string(&f.object)?;
                let locator = f.provenance.source.locator.clone();
                ins_fact.execute(params![
                    f.subject,
                    f.predicate,
                    object_json,
                    doc_id,
                    f.provenance.adapter,
                    f.provenance.confidence as f64,
                    f.provenance.observed_at,
                    locator,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Single-pass conflict detection over the full facts table.
    ///
    /// Only flags pairs where `predicate_cardinality` declares the
    /// (predicate, entity_type) combo to be `scalar`. Pairs with no entry,
    /// or declared `list`, never flag. `ref:*` predicates are graph edges
    /// and never flag regardless.
    pub fn detect_conflicts_bulk(&self) -> Result<usize, StoreError> {
        let n = self.conn.execute(
            "INSERT INTO conflicts(subject, predicate, fact_a_id, fact_b_id)
             SELECT a.subject, a.predicate, a.id, b.id
             FROM facts a
             JOIN facts b
               ON a.subject = b.subject
              AND a.predicate = b.predicate
              AND a.id < b.id
             JOIN entities e ON e.id = a.subject
             JOIN predicate_cardinality pc
               ON pc.predicate = a.predicate
              AND pc.entity_type = e.entity_type
             WHERE a.object_json <> b.object_json
               AND a.predicate NOT LIKE 'ref:%'
               AND pc.cardinality = 'scalar'
               AND NOT EXISTS (
                 SELECT 1 FROM conflicts c
                 WHERE c.fact_a_id = a.id AND c.fact_b_id = b.id
               )",
            [],
        )?;
        Ok(n)
    }

    /// Insert or replace cardinality decision for one (predicate, entity_type) pair.
    pub fn upsert_cardinality(
        &self,
        predicate: &str,
        entity_type: &str,
        cardinality: &str,
        confidence: f64,
        decided_by: &str,
        decided_at: i64,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO predicate_cardinality(predicate, entity_type, cardinality, confidence, decided_by, decided_at)
             VALUES(?,?,?,?,?,?)
             ON CONFLICT(predicate, entity_type) DO UPDATE SET
               cardinality=excluded.cardinality,
               confidence=excluded.confidence,
               decided_by=excluded.decided_by,
               decided_at=excluded.decided_at",
            params![predicate, entity_type, cardinality, confidence, decided_by, decided_at],
        )?;
        Ok(())
    }

    pub fn get_cardinality(
        &self,
        predicate: &str,
        entity_type: &str,
    ) -> Result<Option<String>, StoreError> {
        match self.conn.query_row(
            "SELECT cardinality FROM predicate_cardinality WHERE predicate=? AND entity_type=?",
            params![predicate, entity_type],
            |r| r.get::<_, String>(0),
        ) {
            Ok(c) => Ok(Some(c)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// (predicate, entity_type) pairs that have at least one subject holding
    /// ≥2 distinct object values, plus per-subject sample evidence to feed an
    /// LLM classifier. Excludes `ref:*` (always list) and pairs already
    /// classified in `predicate_cardinality`.
    ///
    /// Each pair samples up to `max_subjects` subjects with multi-values,
    /// each with up to `max_values_per_subject` distinct values.
    pub fn multivalue_evidence(
        &self,
        max_subjects: usize,
        max_values_per_subject: usize,
    ) -> Result<Vec<MultiValueEvidence>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT e.entity_type, f.predicate, f.subject, f.object_json
             FROM facts f
             JOIN entities e ON e.id = f.subject
             WHERE f.predicate NOT LIKE 'ref:%'
               AND NOT EXISTS (
                 SELECT 1 FROM predicate_cardinality pc
                 WHERE pc.predicate = f.predicate AND pc.entity_type = e.entity_type
               )
             ORDER BY e.entity_type, f.predicate, f.subject",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;

        // Group: (entity_type, predicate) -> subject -> set<object_json>.
        let mut grouped: std::collections::BTreeMap<
            (String, String),
            std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
        > = std::collections::BTreeMap::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (et, pred, subj, obj) = row;
            grouped
                .entry((et, pred))
                .or_default()
                .entry(subj)
                .or_default()
                .insert(obj);
        }

        let mut out = Vec::new();
        for ((entity_type, predicate), subjects) in grouped {
            let mut samples: Vec<Vec<String>> = Vec::new();
            for (_subj, vals) in subjects {
                if vals.len() < 2 {
                    continue;
                }
                let mut take: Vec<String> =
                    vals.into_iter().take(max_values_per_subject).collect();
                take.sort();
                samples.push(take);
                if samples.len() >= max_subjects {
                    break;
                }
            }
            if samples.is_empty() {
                continue;
            }
            out.push(MultiValueEvidence {
                entity_type,
                predicate,
                samples,
            });
        }
        Ok(out)
    }

    pub fn unresolved_conflicts(&self) -> Result<Vec<ConflictRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, fact_a_id, fact_b_id FROM conflicts WHERE resolved_at IS NULL ORDER BY id",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(ConflictRow {
                    id: r.get(0)?,
                    subject: r.get(1)?,
                    predicate: r.get(2)?,
                    fact_a_id: r.get(3)?,
                    fact_b_id: r.get(4)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn fact_meta(&self, id: i64) -> Result<FactMeta, StoreError> {
        self.conn.query_row(
            "SELECT id, subject, predicate, object_json, adapter, confidence, observed_at, locator FROM facts WHERE id=?",
            params![id],
            |r| Ok(FactMeta {
                id: r.get(0)?,
                subject: r.get(1)?,
                predicate: r.get(2)?,
                object_json: r.get(3)?,
                adapter: r.get(4)?,
                confidence: r.get(5)?,
                observed_at: r.get(6)?,
                locator: r.get(7)?,
            }),
        ).map_err(Into::into)
    }

    /// Single id → entity_type (None if no such entity).
    pub fn entity_type_of(&self, id: &str) -> Result<Option<String>, StoreError> {
        match self.conn.query_row(
            "SELECT entity_type FROM entities WHERE id=?",
            params![id],
            |r| r.get::<_, String>(0),
        ) {
            Ok(t) => Ok(Some(t)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn all_entities(&self) -> Result<Vec<(String, String)>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT id, entity_type FROM entities ORDER BY entity_type, id")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Page entities of one type, optionally filtered by an id-substring `q`.
    /// Returns `(items, total)` where items is `Vec<id>`. Generic over schema.
    pub fn entities_of_type(
        &self,
        entity_type: &str,
        q: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<String>, i64), StoreError> {
        let lim = limit.max(1) as i64;
        let off = offset as i64;
        let (items, total) = match q {
            Some(qstr) if !qstr.is_empty() => {
                let pat = format!("%{}%", qstr);
                let total: i64 = self.conn.query_row(
                    "SELECT count(*) FROM entities WHERE entity_type=? AND id LIKE ?",
                    params![entity_type, pat],
                    |r| r.get(0),
                )?;
                let mut stmt = self.conn.prepare(
                    "SELECT id FROM entities WHERE entity_type=? AND id LIKE ?
                     ORDER BY id LIMIT ? OFFSET ?",
                )?;
                let ids = stmt
                    .query_map(params![entity_type, pat, lim, off], |r| r.get::<_, String>(0))?
                    .filter_map(|r| r.ok())
                    .collect();
                (ids, total)
            }
            _ => {
                let total: i64 = self.conn.query_row(
                    "SELECT count(*) FROM entities WHERE entity_type=?",
                    params![entity_type],
                    |r| r.get(0),
                )?;
                let mut stmt = self.conn.prepare(
                    "SELECT id FROM entities WHERE entity_type=? ORDER BY id LIMIT ? OFFSET ?",
                )?;
                let ids = stmt
                    .query_map(params![entity_type, lim, off], |r| r.get::<_, String>(0))?
                    .filter_map(|r| r.ok())
                    .collect();
                (ids, total)
            }
        };
        Ok((items, total))
    }

    /// Pick a human label for each id. Heuristic, schema-agnostic:
    /// 1. Highest-confidence non-id alias whose source starts with `field:`.
    /// 2. Else highest-confidence alias from any non-id source.
    /// 3. Else the id itself.
    pub fn entity_labels_bulk(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, String>, StoreError> {
        let mut out = std::collections::HashMap::new();
        if ids.is_empty() {
            return Ok(out);
        }
        // Build "?, ?, ?" placeholder list. SQLite caps params per query at
        // 999 by default — chunk to be safe.
        for chunk in ids.chunks(500) {
            let placeholders = std::iter::repeat("?").take(chunk.len()).collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT entity_id, alias, source, confidence FROM entity_aliases
                 WHERE entity_id IN ({}) AND source <> 'id'
                 ORDER BY entity_id, confidence DESC",
                placeholders
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                chunk.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
            let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, f64>(3)?,
                ))
            })?;
            // Two-pass within chunk: prefer field:* sources over derived/email_local.
            let mut field_pick: std::collections::HashMap<String, (f64, String)> =
                std::collections::HashMap::new();
            let mut other_pick: std::collections::HashMap<String, (f64, String)> =
                std::collections::HashMap::new();
            for row in rows.filter_map(|r| r.ok()) {
                let (id, alias, source, conf) = row;
                let bucket = if source.starts_with("field:") {
                    &mut field_pick
                } else {
                    &mut other_pick
                };
                let entry = bucket.entry(id).or_insert((-1.0, String::new()));
                if conf > entry.0 {
                    *entry = (conf, alias);
                }
            }
            for id in chunk {
                if let Some((_, label)) = field_pick.remove(id) {
                    out.insert(id.clone(), label);
                } else if let Some((_, label)) = other_pick.remove(id) {
                    out.insert(id.clone(), label);
                }
            }
        }
        Ok(out)
    }

    pub fn facts_for_subject(&self, subject: &str) -> Result<Vec<FactMeta>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, object_json, adapter, confidence, observed_at, locator
             FROM facts WHERE subject=? ORDER BY predicate, id",
        )?;
        let rows = stmt
            .query_map(params![subject], |r| Ok(FactMeta {
                id: r.get(0)?,
                subject: r.get(1)?,
                predicate: r.get(2)?,
                object_json: r.get(3)?,
                adapter: r.get(4)?,
                confidence: r.get(5)?,
                observed_at: r.get(6)?,
                locator: r.get(7)?,
            }))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Bulk-load all (subject, predicate, object_json) for indexing.
    /// Single SQL query — avoid N+1 over `facts_for_subject`.
    pub fn all_facts_min(&self) -> Result<Vec<(String, String, String)>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT subject, predicate, object_json FROM facts")?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn entity_counts(&self) -> Result<Vec<(String, i64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT entity_type, count(*) FROM entities GROUP BY entity_type ORDER BY 2 DESC",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn predicate_counts(&self, entity_type: Option<&str>) -> Result<Vec<(String, i64)>, StoreError> {
        let (sql, has_param) = match entity_type {
            Some(_) => (
                "SELECT f.predicate, count(*) FROM facts f
                 JOIN entities e ON e.id=f.subject
                 WHERE e.entity_type=?
                 GROUP BY f.predicate ORDER BY 2 DESC",
                true,
            ),
            None => (
                "SELECT predicate, count(*) FROM facts GROUP BY predicate ORDER BY 2 DESC",
                false,
            ),
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows: Vec<(String, i64)> = if has_param {
            stmt.query_map(params![entity_type.unwrap()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect()
        } else {
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .filter_map(|r| r.ok())
                .collect()
        };
        Ok(rows)
    }

    /// Count facts where the predicate matches and the object equals the
    /// given target (matched as both JSON-quoted string and bare value to
    /// cover legacy bare numeric ids).
    pub fn count_facts_with_object(&self, predicate: &str, target: &str) -> Result<i64, StoreError> {
        let quoted = serde_json::to_string(target)?;
        let n: i64 = self.conn.query_row(
            "SELECT count(*) FROM facts WHERE predicate=?1 AND (object_json=?2 OR object_json=?3)",
            params![predicate, quoted, target],
            |r| r.get(0),
        ).unwrap_or(0);
        Ok(n)
    }

    pub fn top_values(&self, predicate: &str, limit: usize) -> Result<Vec<(String, i64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT object_json, count(*) FROM facts WHERE predicate=?
             GROUP BY object_json ORDER BY 2 DESC LIMIT ?",
        )?;
        let rows = stmt
            .query_map(params![predicate, limit as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Bulk variant: returns a map from `target_id` to list of incoming refs.
    /// Single SQL pass — required for any whole-corpus render to avoid N+1 against `facts`.
    pub fn all_incoming_refs(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<(String, String, String, String, f64)>>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT f.predicate, f.subject, COALESCE(e.entity_type, ''), f.adapter, f.confidence, f.object_json
             FROM facts f
             LEFT JOIN entities e ON e.id = f.subject
             WHERE f.predicate LIKE 'ref:%'",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, f64>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?;
        let mut map: std::collections::HashMap<String, Vec<(String, String, String, String, f64)>> =
            std::collections::HashMap::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (predicate, subject, subject_type, adapter, confidence, object_json) = row;
            // Object may be a JSON-encoded string ("xyz"), bare number, or array. Strip
            // surrounding quotes for plain strings; for arrays/objects we skip (those are
            // typically multi-valued FKs that would have been split into individual facts).
            let target = if object_json.starts_with('"') && object_json.ends_with('"') {
                serde_json::from_str::<String>(&object_json).unwrap_or(object_json.clone())
            } else {
                object_json.trim_matches('"').to_string()
            };
            map.entry(target)
                .or_default()
                .push((predicate, subject, subject_type, adapter, confidence));
        }
        Ok(map)
    }

    /// Find facts whose predicate is `ref:*` and whose object equals the given id.
    /// Returns (predicate, subject_id, subject_type, adapter, confidence). Generic across
    /// entity types — relies only on the store schema, not on any naming convention.
    pub fn incoming_refs(
        &self,
        target_id: &str,
    ) -> Result<Vec<(String, String, String, String, f64)>, StoreError> {
        // Object json may be a quoted string ("xyz") or a bare number (42); match both.
        let quoted = serde_json::to_string(target_id)?;
        let mut stmt = self.conn.prepare(
            "SELECT f.predicate, f.subject, COALESCE(e.entity_type, ''), f.adapter, f.confidence
             FROM facts f
             LEFT JOIN entities e ON e.id = f.subject
             WHERE f.predicate LIKE 'ref:%'
               AND (f.object_json = ?1 OR f.object_json = ?2)
             ORDER BY f.subject",
        )?;
        let rows = stmt
            .query_map(params![quoted, target_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, f64>(4)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    // ---- Aliases (Phase B) ----

    pub fn bulk_upsert_aliases(&mut self, rows: &[AliasRow]) -> Result<usize, StoreError> {
        let tx = self.conn.transaction()?;
        let mut n = 0usize;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO entity_aliases(entity_id, alias, alias_norm, source, confidence)
                 VALUES (?,?,?,?,?)
                 ON CONFLICT(entity_id, alias_norm) DO UPDATE SET
                    alias=excluded.alias,
                    source=excluded.source,
                    confidence=MAX(entity_aliases.confidence, excluded.confidence)",
            )?;
            for r in rows {
                stmt.execute(params![r.entity_id, r.alias, r.alias_norm, r.source, r.confidence])?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn clear_aliases(&self) -> Result<(), StoreError> {
        self.conn.execute("DELETE FROM entity_aliases", [])?;
        Ok(())
    }

    /// Drop every fact emitted by the LLM passes (`llm`, `llm-resolve`) and
    /// reset the section cache so the next ingest re-extracts everything from
    /// scratch. Use when prompt / validation rules change.
    pub fn clear_llm_facts(&self) -> Result<usize, StoreError> {
        self.conn.execute(
            "DELETE FROM conflicts WHERE fact_a_id IN (SELECT id FROM facts WHERE adapter IN ('llm','llm-resolve'))
                                       OR fact_b_id IN (SELECT id FROM facts WHERE adapter IN ('llm','llm-resolve'))",
            [],
        )?;
        let n = self.conn.execute(
            "DELETE FROM facts WHERE adapter IN ('llm','llm-resolve')",
            [],
        )?;
        self.conn.execute("DELETE FROM section_cache", [])?;
        // Drop derived aliases the resolver learned from now-stale refs.
        self.conn.execute(
            "DELETE FROM entity_aliases WHERE source='derived'",
            [],
        )?;
        Ok(n)
    }

    /// Drop every fact previously emitted by the `alias-reconcile` adapter.
    /// Use before re-running reconciliation when alias derivation rules
    /// changed and prior cross-schema refs may be stale.
    pub fn clear_alias_reconcile_facts(&self) -> Result<usize, StoreError> {
        // conflicts.fact_a_id / fact_b_id FK forces conflict cleanup first.
        self.conn.execute(
            "DELETE FROM conflicts WHERE fact_a_id IN (SELECT id FROM facts WHERE adapter='alias-reconcile')
                                       OR fact_b_id IN (SELECT id FROM facts WHERE adapter='alias-reconcile')",
            [],
        )?;
        let n = self.conn.execute(
            "DELETE FROM facts WHERE adapter='alias-reconcile'",
            [],
        )?;
        Ok(n)
    }

    /// All aliases as `(alias_norm, entity_id, entity_type, confidence)` joined with entities.
    pub fn all_aliases_with_type(&self) -> Result<Vec<(String, String, String, f64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT a.alias_norm, a.entity_id, e.entity_type, a.confidence
             FROM entity_aliases a JOIN entities e ON e.id = a.entity_id",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, f64>(3)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Lookup entities for one normalized alias.
    pub fn lookup_alias(&self, alias_norm: &str) -> Result<Vec<(String, String, f64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT a.entity_id, e.entity_type, a.confidence
             FROM entity_aliases a JOIN entities e ON e.id = a.entity_id
             WHERE a.alias_norm = ?",
        )?;
        let rows = stmt
            .query_map([alias_norm], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, f64>(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ---- Embeddings (Phase C) ----

    /// Existing entity embedding metadata: `entity_id -> (card_hash, model)`.
    /// Used to diff against fresh card-hashes and decide which entities need
    /// (re-)embedding.
    pub fn entity_embedding_meta(&self) -> Result<std::collections::HashMap<String, (String, String)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT entity_id, card_hash, model FROM entity_embeddings",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
        let mut out = std::collections::HashMap::new();
        for row in rows {
            let (id, hash, model) = row?;
            out.insert(id, (hash, model));
        }
        Ok(out)
    }

    pub fn put_entity_embedding(
        &self,
        entity_id: &str,
        embedding: &[f32],
        model: &str,
        card_hash: &str,
        when: i64,
    ) -> Result<(), StoreError> {
        let blob = embedding_to_blob(embedding);
        self.conn.execute(
            "INSERT INTO entity_embeddings(entity_id, embedding, model, card_hash, created_at)
             VALUES(?,?,?,?,?)
             ON CONFLICT(entity_id) DO UPDATE SET
                 embedding=excluded.embedding,
                 model=excluded.model,
                 card_hash=excluded.card_hash,
                 created_at=excluded.created_at",
            params![entity_id, blob, model, card_hash, when],
        )?;
        Ok(())
    }

    pub fn bulk_put_entity_embeddings(
        &mut self,
        rows: &[(String, Vec<f32>, String, String, i64)],
    ) -> Result<usize, StoreError> {
        let tx = self.conn.transaction()?;
        let mut n = 0usize;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO entity_embeddings(entity_id, embedding, model, card_hash, created_at)
                 VALUES(?,?,?,?,?)
                 ON CONFLICT(entity_id) DO UPDATE SET
                     embedding=excluded.embedding,
                     model=excluded.model,
                     card_hash=excluded.card_hash,
                     created_at=excluded.created_at",
            )?;
            for (id, emb, model, hash, when) in rows {
                let blob = embedding_to_blob(emb);
                stmt.execute(params![id, blob, model, hash, when])?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Stream all entity embeddings for one model: `(entity_id, entity_type, embedding)`.
    /// Brute-force cosine search reads this once per query.
    pub fn entity_embeddings_for_model(
        &self,
        model: &str,
    ) -> Result<Vec<(String, String, Vec<f32>)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ee.entity_id, e.entity_type, ee.embedding
             FROM entity_embeddings ee JOIN entities e ON e.id = ee.entity_id
             WHERE ee.model = ?",
        )?;
        let rows = stmt.query_map([model], |r| {
            let id: String = r.get(0)?;
            let etype: String = r.get(1)?;
            let blob: Vec<u8> = r.get(2)?;
            Ok((id, etype, embedding_from_blob(&blob)))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn entity_embedding_count(&self) -> Result<i64, StoreError> {
        Ok(self.conn.query_row("SELECT count(*) FROM entity_embeddings", [], |r| r.get(0))?)
    }

    pub fn put_section_embedding(
        &self,
        section_hash: &str,
        embedding: &[f32],
        model: &str,
        when: i64,
    ) -> Result<(), StoreError> {
        let blob = embedding_to_blob(embedding);
        self.conn.execute(
            "INSERT INTO section_embeddings(section_hash, embedding, model, created_at)
             VALUES(?,?,?,?)
             ON CONFLICT(section_hash) DO UPDATE SET
                 embedding=excluded.embedding,
                 model=excluded.model,
                 created_at=excluded.created_at",
            params![section_hash, blob, model, when],
        )?;
        Ok(())
    }

    pub fn get_section_embedding(
        &self,
        section_hash: &str,
        model: &str,
    ) -> Result<Option<Vec<f32>>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT embedding FROM section_embeddings WHERE section_hash=? AND model=?",
        )?;
        let mut rows = stmt.query(params![section_hash, model])?;
        if let Some(r) = rows.next()? {
            let blob: Vec<u8> = r.get(0)?;
            Ok(Some(embedding_from_blob(&blob)))
        } else {
            Ok(None)
        }
    }

    /// Bulk-fetch all aliases as `entity_id -> Vec<(alias, confidence)>`. Used
    /// when building per-entity card text for embedding.
    pub fn aliases_grouped(&self) -> Result<std::collections::HashMap<String, Vec<(String, f64)>>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT entity_id, alias, confidence FROM entity_aliases",
        )?;
        let mut out: std::collections::HashMap<String, Vec<(String, f64)>> = Default::default();
        for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, f64>(2)?)))? {
            let (id, alias, conf) = row?;
            out.entry(id).or_default().push((alias, conf));
        }
        Ok(out)
    }

    /// (subject, predicate, object_json) for every ref:* fact. Used to dedup
    /// when emitting reconciliation refs.
    pub fn all_ref_facts(&self) -> Result<Vec<(String, String, String)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT subject, predicate, object_json FROM facts WHERE predicate LIKE 'ref:%'",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Aliases for one entity, including source + confidence, ordered by
    /// confidence desc.
    pub fn aliases_for_subject(&self, entity_id: &str) -> Result<Vec<(String, String, f64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT alias, source, confidence FROM entity_aliases WHERE entity_id=?
             ORDER BY confidence DESC, alias",
        )?;
        let rows = stmt
            .query_map([entity_id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, f64>(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// All aliases as `entity_id -> [(alias, source, confidence)]`. Used by
    /// `build_all` view rendering for the per-entity Aliases section.
    pub fn aliases_grouped_full(&self) -> Result<std::collections::HashMap<String, Vec<(String, String, f64)>>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT entity_id, alias, source, confidence FROM entity_aliases ORDER BY confidence DESC, alias",
        )?;
        let mut out: std::collections::HashMap<String, Vec<(String, String, f64)>> = Default::default();
        for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, f64>(3)?)))? {
            let (id, alias, source, conf) = row?;
            out.entry(id).or_default().push((alias, source, conf));
        }
        Ok(out)
    }

    pub fn add_resolution_block(&self, alias_norm: &str, entity_id: &str, when: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO resolution_blocklist(alias_norm, entity_id, blocked_at)
             VALUES (?, ?, ?)",
            params![alias_norm, entity_id, when],
        )?;
        Ok(())
    }

    pub fn all_resolution_blocks(&self) -> Result<std::collections::HashSet<(String, String)>, StoreError> {
        let mut stmt = self.conn.prepare("SELECT alias_norm, entity_id FROM resolution_blocklist")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
        Ok(rows)
    }

    /// Suppress the LLM-resolve fact identified by (subject, predicate, object)
    /// by zeroing its confidence (soft delete). Hard-deleting would violate the
    /// `conflicts(fact_a_id)` FK; zeroing keeps provenance for audit while
    /// signalling "rejected" to every consumer.
    pub fn suppress_resolution_fact(&self, subject: &str, predicate: &str, object_json: &str) -> Result<usize, StoreError> {
        let n = self.conn.execute(
            "UPDATE facts SET confidence=0
             WHERE adapter='llm-resolve' AND subject=? AND predicate=? AND object_json=?",
            params![subject, predicate, object_json],
        )?;
        Ok(n)
    }

    /// Recent llm-resolve facts. `since` filters by `observed_at >= since`.
    /// Ordered most-recent first.
    pub fn recent_resolutions(&self, since: Option<i64>, limit: usize) -> Result<Vec<ResolutionRow>, StoreError> {
        let (sql, args): (&str, Vec<Box<dyn rusqlite::ToSql>>) = match since {
            Some(t) => (
                "SELECT subject, predicate, object_json, confidence, locator, observed_at
                 FROM facts WHERE adapter='llm-resolve' AND observed_at >= ?
                 ORDER BY observed_at DESC, id DESC LIMIT ?",
                vec![Box::new(t), Box::new(limit as i64)],
            ),
            None => (
                "SELECT subject, predicate, object_json, confidence, locator, observed_at
                 FROM facts WHERE adapter='llm-resolve'
                 ORDER BY observed_at DESC, id DESC LIMIT ?",
                vec![Box::new(limit as i64)],
            ),
        };
        let mut stmt = self.conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_refs), |r| {
                Ok(ResolutionRow {
                    subject: r.get(0)?,
                    predicate: r.get(1)?,
                    object_json: r.get(2)?,
                    confidence: r.get(3)?,
                    locator: r.get(4)?,
                    observed_at: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn alias_count(&self) -> Result<i64, StoreError> {
        Ok(self.conn.query_row("SELECT count(*) FROM entity_aliases", [], |r| r.get(0))?)
    }

    pub fn resolve_conflict(&self, id: i64, resolution: &str, when: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE conflicts SET resolved_at=?, resolution=? WHERE id=?",
            params![when, resolution, id],
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MultiValueEvidence {
    pub entity_type: String,
    pub predicate: String,
    /// Per-subject distinct object_json samples. Inner Vec has length ≥ 2.
    pub samples: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ResolutionRow {
    pub subject: String,
    pub predicate: String,
    pub object_json: String,
    pub confidence: f64,
    pub locator: Option<String>,
    pub observed_at: i64,
}

#[derive(Debug, Clone)]
pub struct AliasRow {
    pub entity_id: String,
    pub alias: String,
    pub alias_norm: String,
    pub source: String,
    pub confidence: f64,
}

// ---- Embedding BLOB encoding (little-endian f32) ----

pub fn embedding_to_blob(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for f in v {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out
}

pub fn embedding_from_blob(b: &[u8]) -> Vec<f32> {
    let n = b.len() / 4;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let bytes = [b[4 * i], b[4 * i + 1], b[4 * i + 2], b[4 * i + 3]];
        out.push(f32::from_le_bytes(bytes));
    }
    out
}

/// Cosine similarity. Returns 0 for empty / mismatched / zero-norm inputs.
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Normalize an alias for lookup: lowercase, ascii-fold, collapse whitespace,
/// trim. Generic — no schema-specific rules.
pub fn normalize_alias(s: &str) -> String {
    let lowered = s.to_lowercase();
    let folded: String = lowered
        .chars()
        .map(|c| match c {
            'à'|'á'|'â'|'ã'|'ä'|'å' => 'a',
            'ç' => 'c',
            'è'|'é'|'ê'|'ë' => 'e',
            'ì'|'í'|'î'|'ï' => 'i',
            'ñ' => 'n',
            'ò'|'ó'|'ô'|'õ'|'ö' => 'o',
            'ù'|'ú'|'û'|'ü' => 'u',
            'ý'|'ÿ' => 'y',
            _ => c,
        })
        .collect();
    let mut out = String::with_capacity(folded.len());
    let mut prev_ws = true;
    for c in folded.chars() {
        if c.is_whitespace() {
            if !prev_ws { out.push(' '); }
            prev_ws = true;
        } else {
            out.push(c);
            prev_ws = false;
        }
    }
    out.trim().to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaStatus {
    Fresh,
    Cached,
    Stale,
}

#[derive(Debug, Clone)]
pub struct ConflictRow {
    pub id: i64,
    pub subject: String,
    pub predicate: String,
    pub fact_a_id: i64,
    pub fact_b_id: i64,
}

#[derive(Debug, Clone)]
pub struct FactMeta {
    pub id: i64,
    pub subject: String,
    pub predicate: String,
    pub object_json: String,
    pub adapter: String,
    pub confidence: f64,
    pub observed_at: i64,
    pub locator: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_blob_roundtrip() {
        let v = vec![0.0_f32, 1.5, -2.25, 1e-7];
        let b = embedding_to_blob(&v);
        assert_eq!(b.len(), v.len() * 4);
        let v2 = embedding_from_blob(&b);
        assert_eq!(v, v2);
    }

    #[test]
    fn cosine_known_values() {
        let a = [1.0_f32, 0.0];
        let b = [0.0_f32, 1.0];
        let c = [1.0_f32, 1.0];
        assert!((cosine(&a, &a) - 1.0).abs() < 1e-6);
        assert!(cosine(&a, &b).abs() < 1e-6);
        assert!((cosine(&a, &c) - 0.7071068).abs() < 1e-5);
        // mismatched dims and zero vectors return 0.0
        assert_eq!(cosine(&a, &[1.0_f32]), 0.0);
        assert_eq!(cosine(&[0.0_f32, 0.0], &a), 0.0);
    }

    #[test]
    fn migration_applies() {
        let s = Store::open_in_memory().unwrap();
        let n: i64 = s
            .conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('entities','facts','documents','conflicts')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 4);
    }
}
