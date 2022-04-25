INSERT INTO {{schema_name}}_edges
(
    source,
    target,
    properties
)
VALUES (
    ?,
    ?,
    json(?)
)

