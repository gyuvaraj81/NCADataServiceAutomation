Add the dates and the source system names in the config/config.yaml like below

project: "ncau-data-newsquery-prd"
project_nq: "ncau-data-newsquery-sit"
dataset: "sdm_think_nq"
dataset_nq: "sdm_think_nq"
startdate: "2025-11-04"
enddate: "2025-11-04"
startdate_nq: "2025-11-05"
enddate_nq: "2025-11-05"
output_csv: "output/"
skip_tables:
  - cart
  - order
  - customer
  - customer_address

Then,

To run the invidual query

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src/main/python/run_query.py --query count

or 

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src/main/python/run_query.py --query schema_compare

or 

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src/main/python/run_query.py --query except_distinct --tablename rcti_wool*

To run all queries in single shot

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src/main/python/run_query.py

or

[ Below is the recommended way of calling as it calls the Karate test cases 
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>set PATH=C:\Program Files\Java\jdk-17\bin;%PATH%

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -version
java version "17.0.12" 2024-07-16 LTS
Java(TM) SE Runtime Environment (build 17.0.12+8-LTS-286)
Java HotSpot(TM) 64-Bit Server VM (build 17.0.12+8-LTS-286, mixed mode, sharing)

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -Dtablename='rcti_c*' -jar karate-1.5.1.jar -t @regression
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -Dtablename='rcti_c*' -jar karate-1.5.1.jar --tag @regression
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -Dtablename='rcti_c*' -jar karate-1.5.1.jar -t @regression features/except_distinct_fullload.feature
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -jar karate-1.5.1.jar features/except_distinct.feature
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython_Refactor>java -Dtablename='rcti_c*' -jar karate-1.5.1.jar features/test_except_distinct.feature
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -jar karate-1.5.1.jar features/count.feature
or
D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>java -jar karate-1.5.1.jar features/schema_compare.feature
]

Exmaple:

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src\main\python\run_query.py --query except_distinct --tablename customer_*
 
(Actually, below is the console output of D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>python src\main\python\run_query.py --query except_distinct --tablename a*)

Started fetching and parsing all the configuration files ...

Running query: except_distinct

▶ Generated SQL:

WITH table_info AS
  (SELECT table_catalog,
          table_schema,
          TABLE_NAME,
          STRING_AGG(CONCAT('`', COLUMN_NAME, '`'), ','
                     ORDER BY ordinal_position) AS columns
   FROM `ncau-data-newsquery-prd.sdm_think_nq.INFORMATION_SCHEMA.COLUMNS`
   WHERE COLUMN_NAME NOT IN ('nq_batch_id',
                             'dgw_batch_id',
                             'dw_batch_id',
                             'dw_ingest_time',
                             'dw_partition_date',
                             'dw_publish_time',
                             'dw_source_object_name',
                             'soft_error_fields',
                             'extra_data_payload')
     AND TABLE_NAME NOT IN ('appc_send_log',
                            'appc_send_log_0715',
                            'contactsf',
                            'emailobject',
                            'pushmessagedetail',
                            'smsdetailsummary',
                            'cart',
                            'order',
                            'customer',
                            'customer_address')
   GROUP BY table_catalog,
            table_schema,
            TABLE_NAME)
SELECT 'SIT-prod' AS scenario,
       TABLE_NAME,
       CONCAT('SELECT "', TABLE_NAME, '" AS table_name, ', columns, ' FROM `', 'ncau-data-newsquery-sit', '.', 'sdm_think_nq',
 '.', TABLE_NAME, '` ', 'WHERE dw_partition_date BETWEEN "', '2025-11-05', '" AND "', '2025-11-10', '" ', ' EXCEPT DISTINCT ',
 'SELECT "', TABLE_NAME, '" AS table_name, ', columns, ' FROM `', 'ncau-data-newsquery-prd', '.', 'sdm_think_nq', '.', TABLE_N
AME, '` ', 'WHERE dw_partition_date BETWEEN "', '2025-11-04', '" AND "', '2025-11-09', '";') AS testcase
FROM table_info
UNION ALL
SELECT 'prod-SIT' AS scenario,
       TABLE_NAME,
       CONCAT('SELECT "', TABLE_NAME, '" AS table_name, ', columns, ' FROM `', 'ncau-data-newsquery-prd', '.', 'sdm_think_nq',
 '.', TABLE_NAME, '` ', 'WHERE dw_partition_date BETWEEN "', '2025-11-04', '" AND "', '2025-11-09', '" ', ' EXCEPT DISTINCT ',
 'SELECT "', TABLE_NAME, '" AS table_name, ', columns, ' FROM `', 'ncau-data-newsquery-sit', '.', 'sdm_think_nq', '.', TABLE_N
AME, '` ', 'WHERE dw_partition_date BETWEEN "', '2025-11-05', '" AND "', '2025-11-10', '";') AS testcase
FROM table_info
ORDER BY TABLE_NAME;
Executing query... - (2.4s)D:\Users\ganesany\AppData\Roaming\Python\Python311\site-packages\google\cloud\bigquery\table.py:199
4: UserWarning: BigQuery Storage module not found, fetch data with the REST endpoint instead.
  warnings.warn(
✅ Running testcases for tables starting with 'a'

Executing EXCEPT DISTINCT testcases...


▶▶▶ Running testcase [SIT-prod] for table → address_status ▶▶▶

▶ Generated SQL:

SELECT "address_status" AS TABLE_NAME,
       `address_status`,
       `description`,
       `address_undeliverable`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.address_status`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "address_status" AS TABLE_NAME,
       `address_status`,
       `description`,
       `address_undeliverable`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.address_status`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → address_status ▶▶▶

▶ Generated SQL:

SELECT "address_status" AS TABLE_NAME,
       `address_status`,
       `description`,
       `address_undeliverable`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.address_status`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "address_status" AS TABLE_NAME,
       `address_status`,
       `description`,
       `address_undeliverable`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.address_status`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → address_type ▶▶▶

▶ Generated SQL:

SELECT "address_type" AS TABLE_NAME,
       `address_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.address_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "address_type" AS TABLE_NAME,
       `address_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.address_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → address_type ▶▶▶

▶ Generated SQL:

SELECT "address_type" AS TABLE_NAME,
       `address_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.address_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "address_type" AS TABLE_NAME,
       `address_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.address_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → adjustment_type ▶▶▶

▶ Generated SQL:

SELECT "adjustment_type" AS TABLE_NAME,
       `adjustment_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.adjustment_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "adjustment_type" AS TABLE_NAME,
       `adjustment_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.adjustment_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → adjustment_type ▶▶▶

▶ Generated SQL:

SELECT "adjustment_type" AS TABLE_NAME,
       `adjustment_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.adjustment_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "adjustment_type" AS TABLE_NAME,
       `adjustment_type`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.adjustment_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → agency ▶▶▶

▶ Generated SQL:

SELECT "agency" AS TABLE_NAME,
       `customer_id`,
       `payment_threshold`,
       `agency_code`,
       `remit`,
       `discounts`,
       `accept_ord`,
       `agency_pays_tax`,
       `tax_based_on_gross`,
       `agency_bill_to`,
       `agency_renew_to`,
       `ren_commission`,
       `new_commission`,
       `company`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.agency`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "agency" AS TABLE_NAME,
       `customer_id`,
       `payment_threshold`,
       `agency_code`,
       `remit`,
       `discounts`,
       `accept_ord`,
       `agency_pays_tax`,
       `tax_based_on_gross`,
       `agency_bill_to`,
       `agency_renew_to`,
       `ren_commission`,
       `new_commission`,
       `company`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.agency`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → agency ▶▶▶

▶ Generated SQL:

SELECT "agency" AS TABLE_NAME,
       `customer_id`,
       `payment_threshold`,
       `agency_code`,
       `remit`,
       `discounts`,
       `accept_ord`,
       `agency_pays_tax`,
       `tax_based_on_gross`,
       `agency_bill_to`,
       `agency_renew_to`,
       `ren_commission`,
       `new_commission`,
       `company`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.agency`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "agency" AS TABLE_NAME,
       `customer_id`,
       `payment_threshold`,
       `agency_code`,
       `remit`,
       `discounts`,
       `accept_ord`,
       `agency_pays_tax`,
       `tax_based_on_gross`,
       `agency_bill_to`,
       `agency_renew_to`,
       `ren_commission`,
       `new_commission`,
       `company`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.agency`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
⚠️ Saved failing SQL → output/failed_testcases\prod-SIT_agency_FAILED.sql
❌ Testcase failed → 1 differing row(s) found

▶▶▶ Running testcase [SIT-prod] for table → alternate_content ▶▶▶

▶ Generated SQL:

SELECT "alternate_content" AS TABLE_NAME,
       `table_name`,
       `key_part1`,
       `key_part2`,
       `key_part3`,
       `column_name`,
       `language_code`,
       `alternate_content`
FROM `ncau-data-newsquery-sit.sdm_think_nq.alternate_content`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "alternate_content" AS TABLE_NAME,
       `table_name`,
       `key_part1`,
       `key_part2`,
       `key_part3`,
       `column_name`,
       `language_code`,
       `alternate_content`
FROM `ncau-data-newsquery-prd.sdm_think_nq.alternate_content`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → alternate_content ▶▶▶

▶ Generated SQL:

SELECT "alternate_content" AS TABLE_NAME,
       `table_name`,
       `key_part1`,
       `key_part2`,
       `key_part3`,
       `column_name`,
       `language_code`,
       `alternate_content`
FROM `ncau-data-newsquery-prd.sdm_think_nq.alternate_content`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "alternate_content" AS TABLE_NAME,
       `table_name`,
       `key_part1`,
       `key_part2`,
       `key_part3`,
       `column_name`,
       `language_code`,
       `alternate_content`
FROM `ncau-data-newsquery-sit.sdm_think_nq.alternate_content`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_basic_price ▶▶▶

▶ Generated SQL:

SELECT "audit_basic_price" AS TABLE_NAME,
       `audit_basic_price_id`,
       `low_percent`,
       `high_percent`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_basic_price`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_basic_price" AS TABLE_NAME,
       `audit_basic_price_id`,
       `low_percent`,
       `high_percent`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_basic_price`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_basic_price ▶▶▶

▶ Generated SQL:

SELECT "audit_basic_price" AS TABLE_NAME,
       `audit_basic_price_id`,
       `low_percent`,
       `high_percent`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_basic_price`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_basic_price" AS TABLE_NAME,
       `audit_basic_price_id`,
       `low_percent`,
       `high_percent`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_basic_price`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_duration ▶▶▶

▶ Generated SQL:

SELECT "audit_duration" AS TABLE_NAME,
       `audit_duration_id`,
       `low_n_months`,
       `high_n_months`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_duration`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_duration" AS TABLE_NAME,
       `audit_duration_id`,
       `low_n_months`,
       `high_n_months`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_duration`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_duration ▶▶▶

▶ Generated SQL:

SELECT "audit_duration" AS TABLE_NAME,
       `audit_duration_id`,
       `low_n_months`,
       `high_n_months`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_duration`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_duration" AS TABLE_NAME,
       `audit_duration_id`,
       `low_n_months`,
       `high_n_months`,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_duration`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_name_title ▶▶▶

▶ Generated SQL:

SELECT "audit_name_title" AS TABLE_NAME,
       `audit_name_title_id`,
       `audit_name_title`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_name_title`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_name_title" AS TABLE_NAME,
       `audit_name_title_id`,
       `audit_name_title`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_name_title`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_name_title ▶▶▶

▶ Generated SQL:

SELECT "audit_name_title" AS TABLE_NAME,
       `audit_name_title_id`,
       `audit_name_title`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_name_title`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_name_title" AS TABLE_NAME,
       `audit_name_title_id`,
       `audit_name_title`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_name_title`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_pub_group ▶▶▶

▶ Generated SQL:

SELECT "audit_pub_group" AS TABLE_NAME,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_pub_group`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_pub_group" AS TABLE_NAME,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_pub_group`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_pub_group ▶▶▶

▶ Generated SQL:

SELECT "audit_pub_group" AS TABLE_NAME,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_pub_group`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_pub_group" AS TABLE_NAME,
       `audit_pub_group`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_pub_group`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_qual_source ▶▶▶

▶ Generated SQL:

SELECT "audit_qual_source" AS TABLE_NAME,
       `audit_qual_source_id`,
       `audit_qual_source`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_qual_source`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_qual_source" AS TABLE_NAME,
       `audit_qual_source_id`,
       `audit_qual_source`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_qual_source`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_qual_source ▶▶▶

▶ Generated SQL:

SELECT "audit_qual_source" AS TABLE_NAME,
       `audit_qual_source_id`,
       `audit_qual_source`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_qual_source`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_qual_source" AS TABLE_NAME,
       `audit_qual_source_id`,
       `audit_qual_source`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_qual_source`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_report ▶▶▶

▶ Generated SQL:

SELECT "audit_report" AS TABLE_NAME,
       `audit_report_id`,
       `audit_report`,
       `audit_paragraph_axis1`,
       `audit_paragraph_axis2`,
       `dem_question_id1`,
       `dem_question_id2`,
       `report_type`,
       `audit_pub_group`,
       `description`,
       `report`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_report`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_report" AS TABLE_NAME,
       `audit_report_id`,
       `audit_report`,
       `audit_paragraph_axis1`,
       `audit_paragraph_axis2`,
       `dem_question_id1`,
       `dem_question_id2`,
       `report_type`,
       `audit_pub_group`,
       `description`,
       `report`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_report`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_report ▶▶▶

▶ Generated SQL:

SELECT "audit_report" AS TABLE_NAME,
       `audit_report_id`,
       `audit_report`,
       `audit_paragraph_axis1`,
       `audit_paragraph_axis2`,
       `dem_question_id1`,
       `dem_question_id2`,
       `report_type`,
       `audit_pub_group`,
       `description`,
       `report`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_report`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_report" AS TABLE_NAME,
       `audit_report_id`,
       `audit_report`,
       `audit_paragraph_axis1`,
       `audit_paragraph_axis2`,
       `dem_question_id1`,
       `dem_question_id2`,
       `report_type`,
       `audit_pub_group`,
       `description`,
       `report`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_report`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_sales_channel ▶▶▶

▶ Generated SQL:

SELECT "audit_sales_channel" AS TABLE_NAME,
       `audit_sales_channel_id`,
       `audit_sales_channel`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sales_channel`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_sales_channel" AS TABLE_NAME,
       `audit_sales_channel_id`,
       `audit_sales_channel`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sales_channel`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_sales_channel ▶▶▶

▶ Generated SQL:

SELECT "audit_sales_channel" AS TABLE_NAME,
       `audit_sales_channel_id`,
       `audit_sales_channel`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sales_channel`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_sales_channel" AS TABLE_NAME,
       `audit_sales_channel_id`,
       `audit_sales_channel`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sales_channel`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_sub_region ▶▶▶

▶ Generated SQL:

SELECT "audit_sub_region" AS TABLE_NAME,
       `audit_sub_region_id`,
       `region_list`,
       `region`,
       `description`,
       `mru_audit_sub_region_zip_seq`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sub_region`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_sub_region" AS TABLE_NAME,
       `audit_sub_region_id`,
       `region_list`,
       `region`,
       `description`,
       `mru_audit_sub_region_zip_seq`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sub_region`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_sub_region ▶▶▶

▶ Generated SQL:

SELECT "audit_sub_region" AS TABLE_NAME,
       `audit_sub_region_id`,
       `region_list`,
       `region`,
       `description`,
       `mru_audit_sub_region_zip_seq`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sub_region`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_sub_region" AS TABLE_NAME,
       `audit_sub_region_id`,
       `region_list`,
       `region`,
       `description`,
       `mru_audit_sub_region_zip_seq`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sub_region`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_sub_region_zip ▶▶▶

▶ Generated SQL:

SELECT "audit_sub_region_zip" AS TABLE_NAME,
       `audit_sub_region_id`,
       `audit_sub_region_zip_seq`,
       `low_zip`,
       `high_zip`,
       `state`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sub_region_zip`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_sub_region_zip" AS TABLE_NAME,
       `audit_sub_region_id`,
       `audit_sub_region_zip_seq`,
       `low_zip`,
       `high_zip`,
       `state`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sub_region_zip`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_sub_region_zip ▶▶▶

▶ Generated SQL:

SELECT "audit_sub_region_zip" AS TABLE_NAME,
       `audit_sub_region_id`,
       `audit_sub_region_zip_seq`,
       `low_zip`,
       `high_zip`,
       `state`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_sub_region_zip`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_sub_region_zip" AS TABLE_NAME,
       `audit_sub_region_id`,
       `audit_sub_region_zip_seq`,
       `low_zip`,
       `high_zip`,
       `state`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_sub_region_zip`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → audit_subscription_type ▶▶▶

▶ Generated SQL:

SELECT "audit_subscription_type" AS TABLE_NAME,
       `audit_subscription_type_id`,
       `audit_subscription_type`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_subscription_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "audit_subscription_type" AS TABLE_NAME,
       `audit_subscription_type_id`,
       `audit_subscription_type`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_subscription_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → audit_subscription_type ▶▶▶

▶ Generated SQL:

SELECT "audit_subscription_type" AS TABLE_NAME,
       `audit_subscription_type_id`,
       `audit_subscription_type`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-prd.sdm_think_nq.audit_subscription_type`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "audit_subscription_type" AS TABLE_NAME,
       `audit_subscription_type_id`,
       `audit_subscription_type`,
       `audit_pub_group`,
       `qp`,
       `qf`,
       `nqp`,
       `nqf`,
       `description`,
       `row_version`
FROM `ncau-data-newsquery-sit.sdm_think_nq.audit_subscription_type`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [SIT-prod] for table → aux_field ▶▶▶

▶ Generated SQL:

SELECT "aux_field" AS TABLE_NAME,
       `table_name`,
       `column_name`,
       `column_title`,
       `column_datatype`,
       `column_length`,
       `column_precision`,
       `row_version`,
       `value_list`,
       `lookup_table_name`,
       `lookup_display_column_name`,
       `lookup_value_column_name`,
       `renew_carry_over`,
       `custsvc_edit_disposition`,
       `default_value`,
       `use_date_picker`
FROM `ncau-data-newsquery-sit.sdm_think_nq.aux_field`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10"
EXCEPT DISTINCT
SELECT "aux_field" AS TABLE_NAME,
       `table_name`,
       `column_name`,
       `column_title`,
       `column_datatype`,
       `column_length`,
       `column_precision`,
       `row_version`,
       `value_list`,
       `lookup_table_name`,
       `lookup_display_column_name`,
       `lookup_value_column_name`,
       `renew_carry_over`,
       `custsvc_edit_disposition`,
       `default_value`,
       `use_date_picker`
FROM `ncau-data-newsquery-prd.sdm_think_nq.aux_field`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

▶▶▶ Running testcase [prod-SIT] for table → aux_field ▶▶▶

▶ Generated SQL:

SELECT "aux_field" AS TABLE_NAME,
       `table_name`,
       `column_name`,
       `column_title`,
       `column_datatype`,
       `column_length`,
       `column_precision`,
       `row_version`,
       `value_list`,
       `lookup_table_name`,
       `lookup_display_column_name`,
       `lookup_value_column_name`,
       `renew_carry_over`,
       `custsvc_edit_disposition`,
       `default_value`,
       `use_date_picker`
FROM `ncau-data-newsquery-prd.sdm_think_nq.aux_field`
WHERE dw_partition_date BETWEEN "2025-11-04" AND "2025-11-09"
EXCEPT DISTINCT
SELECT "aux_field" AS TABLE_NAME,
       `table_name`,
       `column_name`,
       `column_title`,
       `column_datatype`,
       `column_length`,
       `column_precision`,
       `row_version`,
       `value_list`,
       `lookup_table_name`,
       `lookup_display_column_name`,
       `lookup_value_column_name`,
       `renew_carry_over`,
       `custsvc_edit_disposition`,
       `default_value`,
       `use_date_picker`
FROM `ncau-data-newsquery-sit.sdm_think_nq.aux_field`
WHERE dw_partition_date BETWEEN "2025-11-05" AND "2025-11-10";
--------------------------------------------------------------------------------
✔ Testcase passed → no differences found

📁 Testcase results saved: output\testcase_results\sdm_think_nq_except_distinct_20251215_202623.csv
Finished query: except_distinct in 55.28 seconds

D:\Users\ganesany\Desktop\YG\GIT\DGWDataServiceAutomationPython>^A

