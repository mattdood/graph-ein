DELETE FROM {{schema_name}}_nodes
WHERE
    "id" IN {{node_ids}}
;

