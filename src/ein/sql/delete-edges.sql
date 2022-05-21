DELETE FROM {{schema_name}}_edges
WHERE
    source = ?
    OR
    target = ?
;

