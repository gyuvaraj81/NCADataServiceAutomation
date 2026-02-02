WITH prod AS (
    SELECT table_name, PARSE_DATE('%Y%m%d', partition_id) AS partition_date, total_rows
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE partition_id NOT LIKE '%NULL%'
    AND PARSE_DATE('%Y%m%d', partition_id) BETWEEN DATE('{startdate}') AND DATE('{enddate}')
),
prod_nq AS (
    SELECT table_name, PARSE_DATE('%Y%m%d', partition_id) AS partition_date, total_rows
    FROM `{project_nq}.{dataset_nq}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE partition_id NOT LIKE '%NULL%'
    AND PARSE_DATE('%Y%m%d', partition_id) BETWEEN DATE('{startdate_nq}') AND DATE('{enddate_nq}')
),
info_table AS (
    SELECT table_name
    FROM `{project_nq}.{dataset_nq}.INFORMATION_SCHEMA.TABLES`
)

SELECT 
    i.table_name,
    a.partition_date AS sit_prtn_date,
    b.partition_date AS prd_prtn_date,
    a.total_rows AS sit_count,
    b.total_rows AS prd_count,
    a.total_rows - b.total_rows AS diff
FROM info_table i
LEFT JOIN prod_nq a ON i.table_name = a.table_name
LEFT JOIN prod b ON i.table_name = b.table_name
WHERE i.table_name NOT IN ({skip_tables_sql})
ORDER BY 1, 3;