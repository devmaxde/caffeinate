-- Phase B + C: alias index + embedding storage.
-- All IF NOT EXISTS so the file can be replayed on every open alongside 0001.

CREATE TABLE IF NOT EXISTS entity_aliases (
    entity_id   TEXT NOT NULL,
    alias       TEXT NOT NULL,
    alias_norm  TEXT NOT NULL,
    source      TEXT NOT NULL,           -- 'id' | 'field:<name>' | 'email_local' | 'derived'
    confidence  REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY (entity_id, alias_norm)
);
CREATE INDEX IF NOT EXISTS idx_alias_norm ON entity_aliases(alias_norm);
CREATE INDEX IF NOT EXISTS idx_alias_entity ON entity_aliases(entity_id);

CREATE TABLE IF NOT EXISTS entity_embeddings (
    entity_id    TEXT PRIMARY KEY,
    embedding    BLOB NOT NULL,
    model        TEXT NOT NULL,
    card_hash    TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entity_embed_model ON entity_embeddings(model);

CREATE TABLE IF NOT EXISTS section_embeddings (
    section_hash TEXT PRIMARY KEY,
    embedding    BLOB NOT NULL,
    model        TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);

-- Phase H.3: human-rejected (surface_form, entity) pairs. Alias derivation
-- skips matching pairs so a rejection sticks across re-ingests.
CREATE TABLE IF NOT EXISTS resolution_blocklist (
    alias_norm TEXT NOT NULL,
    entity_id  TEXT NOT NULL,
    blocked_at INTEGER NOT NULL,
    PRIMARY KEY (alias_norm, entity_id)
);
