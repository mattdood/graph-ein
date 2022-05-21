SELECT
    name
FROM
    sqlite_master
WHERE
    type = 'table'
    AND
    tbl_name LIKE '%' || ? || '%'
;
