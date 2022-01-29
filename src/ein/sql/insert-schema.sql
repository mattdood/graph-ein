CREATE TABLE IF NOT EXISTS {{schema_name}}_nodes (
    id TEXT NOT NULL PRIMARY KEY,
    body TEXT
);

CREATE INDEX IF NOT EXISTS idx_id ON {{schema_name}}_nodes(id);

CREATE TABLE IF NOT EXISTS {{schema_name}}_edges (
    source TEXT,
    target TEXT,
    properties TEXT,
    UNIQUE(source, target, properties) ON CONFLICT REPLACE,
    FOREIGN KEY(source) REFERENCES nodes(id),
    FOREIGN KEY(target) REFERENCES nodes(id)
);

CREATE INDEX IF NOT EXISTS idx_source ON {{schema_name}}_edges(source);
CREATE INDEX IF NOT EXISTS idx_target ON {{schema_name}}_edges(target);

