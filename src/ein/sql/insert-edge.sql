INSERT INTO {{schema_name}}_edges
(
    source,
    source_schema,
    target,
    target_schema,
    properties
)
VALUES (
    ?,
    ?,
    ?,
    ?,
    json(?)
)
;

