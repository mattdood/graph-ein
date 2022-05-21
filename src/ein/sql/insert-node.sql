INSERT INTO {{schema_name}}_nodes
(
    id,
    body
)
VALUES (
    ?,
    json(?)
)
;

