UPDATE {{schema_name}}_edges
SET properties = json(?)
WHERE
    source = ?
    AND
    target = ?
;

