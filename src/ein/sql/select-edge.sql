SELECT
    *
FROM
    {{schema_name}}_edges
WHERE
    "source" = ?
    AND
    "target" = ?
;

