CREATE TABLE IF NOT EXISTS documents (
    id           INTEGER PRIMARY KEY,
    path         TEXT NOT NULL,
    adapter      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    ingested_at  INTEGER NOT NULL,
    UNIQUE(path, content_hash)
);

CREATE TABLE IF NOT EXISTS entities (
    id          TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);

CREATE TABLE IF NOT EXISTS facts (
    id           INTEGER PRIMARY KEY,
    subject      TEXT NOT NULL,
    predicate    TEXT NOT NULL,
    object_json  TEXT NOT NULL,
    document_id  INTEGER REFERENCES documents(id),
    adapter      TEXT NOT NULL,
    confidence   REAL NOT NULL DEFAULT 1.0,
    observed_at  INTEGER NOT NULL,
    locator      TEXT
);
CREATE INDEX IF NOT EXISTS idx_facts_sp ON facts(subject, predicate);

CREATE TABLE IF NOT EXISTS section_cache (
    section_hash TEXT PRIMARY KEY,
    extracted_at INTEGER NOT NULL,
    fact_count   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conflicts (
    id           INTEGER PRIMARY KEY,
    subject      TEXT NOT NULL,
    predicate    TEXT NOT NULL,
    fact_a_id    INTEGER NOT NULL REFERENCES facts(id),
    fact_b_id    INTEGER NOT NULL REFERENCES facts(id),
    resolved_at  INTEGER,
    resolution   TEXT
);

CREATE TABLE IF NOT EXISTS predicate_cardinality (
    predicate   TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    cardinality TEXT NOT NULL CHECK (cardinality IN ('list','scalar')),
    confidence  REAL NOT NULL DEFAULT 1.0,
    decided_by  TEXT NOT NULL,
    decided_at  INTEGER NOT NULL,
    PRIMARY KEY (predicate, entity_type)
);
