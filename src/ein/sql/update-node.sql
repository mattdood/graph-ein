UPDATE {{schema_name}}_nodes
SET body = json(?)
WHERE
    "id" = ?
;

