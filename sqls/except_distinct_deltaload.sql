WITH table_info AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        STRING_AGG(CONCAT('`', column_name, '`'), ',' ORDER BY ordinal_position) AS columns
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE column_name NOT IN (
        'nq_batch_id','dgw_batch_id','dw_batch_id','dw_ingest_time',
        'dw_partition_date','dw_publish_time','dw_source_object_name',
        'soft_error_fields','extra_data_payload'
    )
    AND table_name NOT IN (
        'appc_send_log','appc_send_log_0715','contactsf',
        'emailobject','pushmessagedetail','smsdetailsummary', {skip_tables_sql}
    )
    AND table_name IN ( {deltaload_tables_sql} )
    GROUP BY table_catalog, table_schema, table_name
)

SELECT
    'SIT-prod' AS scenario,
    table_name,
    CONCAT(
        'SELECT "', table_name, '" AS table_name, ', columns,
        ' FROM `', '{project_nq}', '.', '{dataset_nq}', '.', table_name, '` ',
        'WHERE dw_partition_date BETWEEN "', '{startdate_nq}', '" AND "', '{enddate_nq}', '" ',
        ' EXCEPT DISTINCT ',
        'SELECT "', table_name, '" AS table_name, ', columns,
        ' FROM `', '{project}', '.', '{dataset}', '.', table_name, '` ',
        'WHERE dw_partition_date BETWEEN "', '{startdate}', '" AND "', '{enddate}', '";'
    ) AS testcase
FROM table_info

UNION ALL

SELECT
    'prod-SIT' AS scenario,
    table_name,
    CONCAT(
        'SELECT "', table_name, '" AS table_name, ', columns,
        ' FROM `', '{project}', '.', '{dataset}', '.', table_name, '` ',
        'WHERE dw_partition_date BETWEEN "', '{startdate}', '" AND "', '{enddate}', '" ',
        ' EXCEPT DISTINCT ',
        'SELECT "', table_name, '" AS table_name, ', columns,
        ' FROM `', '{project_nq}', '.', '{dataset_nq}', '.', table_name, '` ',
        'WHERE dw_partition_date BETWEEN "', '{startdate_nq}', '" AND "', '{enddate_nq}', '";'
    ) AS testcase
FROM table_info

ORDER BY table_name;
