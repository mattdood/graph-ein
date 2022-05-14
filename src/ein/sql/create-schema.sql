-- enable foreign key enforcement
PRAGMA foreign_keys = ON;

-- create nodes table with index
CREATE TABLE IF NOT EXISTS {{schema_name}}_nodes (
    id TEXT NOT NULL PRIMARY KEY,
    body JSON
);

CREATE INDEX IF NOT EXISTS idx_id ON {{schema_name}}_nodes(id);

-- create edges table with weighted properties
CREATE TABLE IF NOT EXISTS {{schema_name}}_edges (
    source TEXT,
    target TEXT,
    properties TEXT,
    UNIQUE(source, target, properties) ON CONFLICT REPLACE,
    FOREIGN KEY(source) REFERENCES nodes(id) ON DELETE CASCADE,
    FOREIGN KEY(target) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_source ON {{schema_name}}_edges(source);
CREATE INDEX IF NOT EXISTS idx_target ON {{schema_name}}_edges(target);

