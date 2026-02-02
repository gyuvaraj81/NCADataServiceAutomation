---*** SQL FAILED DURING EXECUTION ***---

WITH prod AS
  (SELECT TABLE_NAME,
          PARSE_DATE('%Y%m%d', partition_id) AS partition_date,
          total_rows
   FROM `ncau-data-newsquery-prd.sdm_rcti_nq.INFORMATION_SCHEMA.PARTITIONS`
   WHERE partition_id NOT LIKE '%NULL%'
     AND PARSE_DATE('%Y%m%d', partition_id) BETWEEN DATE('2025-11-04') AND DATE('2025-11-09')),
     prod_nq AS
  (SELECT TABLE_NAME,
          PARSE_DATE('%Y%m%d', partition_id) AS partition_date,
          total_rows
   FROM `ncau-data-newsquery-sit.sdm_rcti_nq.INFORMATION_SCHEMA.PARTITIONS`
   WHERE partition_id NOT LIKE '%NULL%'
     AND PARSE_DATE('%Y%m%d', partition_id) BETWEEN DATE('2025-11-05') AND DATE('2025-11-10')),
     info_table AS
  (SELECT TABLE_NAME
   FROM `ncau-data-newsquery-sit.sdm_rcti_nq.INFORMATION_SCHEMA.TABLES`)
SELECT i.table_name,
       a.partition_date AS sit_prtn_date,
       b.partition_date AS prd_prtn_date,
       a.total_rows AS sit_count,
       b.total_rows AS prd_count,
       a.total_rows - b.total_rows AS diff
FROM info_table i
LEFT JOIN prod_nq a ON i.table_name = a.table_name
LEFT JOIN prod b ON i.table_name = b.table_name
WHERE i.table_name NOT IN ('cart',
                           'order')
ORDER BY 1,
         3;

-----------------------------------------------------------
                ---*** ERROR MESSAGE ***---                
-----------------------------------------------------------
Diff detected in count query
-----------------------------------------------------------

