WITH prod_schema AS (
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
),
sit_schema AS (
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM `{project_nq}.{dataset_nq}.INFORMATION_SCHEMA.COLUMNS`
),
all_columns AS (
    -- Get all unique table/column combinations from both datasets
    SELECT table_name, column_name FROM prod_schema
    UNION DISTINCT
    SELECT table_name, column_name FROM sit_schema
)

SELECT
    c.table_name,
    c.column_name,
    p.data_type AS prod_type,
    n.data_type AS prod_nq_type,
    p.is_nullable AS prod_nullable,
    n.is_nullable AS prod_nq_nullable,
    CASE
        WHEN p.data_type IS DISTINCT FROM n.data_type THEN 'TYPE_MISMATCH'
        WHEN p.is_nullable IS DISTINCT FROM n.is_nullable THEN 'NULLABILITY_MISMATCH'
        ELSE 'MATCH'
    END AS diff_status
FROM all_columns c
LEFT JOIN prod_schema p
    ON c.table_name = p.table_name AND c.column_name = p.column_name
LEFT JOIN sit_schema n
    ON c.table_name = n.table_name AND c.column_name = n.column_name
WHERE c.table_name NOT IN ('cart','order','customer','customer_address')
ORDER BY c.table_name, c.column_name;
