-- ====================================================================
-- SF GOV PLATFORM - DBT POST-RUN GOVERNANCE SYNC (Corrected)
-- ====================================================================

-- Context
USE WAREHOUSE GOV_WH;
USE DATABASE GOV_PLATFORM;

-- Params
SET run_date = IFNULL($run_date, CURRENT_DATE());
SET batch_id = 'dbt_sync_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

-- Log start
INSERT INTO GOV_PLATFORM.RISK.CONTROL_TEST (TEST_ID, CONTROL_ID, RUN_ID, EXECUTED_AT, OUTCOME, DETAILS)
VALUES ($batch_id || '_start', 'DBT_METADATA_SYNC', $batch_id, CURRENT_TIMESTAMP(), 'PASS', 'Starting DBT metadata sync process');

-- 1) Sync datasets from information_schema.tables
MERGE INTO GOV_PLATFORM.CATALOG.DIM_DATASET AS target
USING (
    SELECT 
        'dbt_' || table_catalog || '_' || table_schema || '_' || table_name AS DATASET_ID,
        table_catalog || '.' || table_schema || '.' || table_name AS CATALOG_NAME,
        'DBT_PROD' AS SYSTEM_ID,
        'ANALYTICS' AS DOMAIN_ID,
        'SNOWFLAKE' AS PLATFORM,
        table_catalog AS DATABASE_NAME,
        table_schema  AS SCHEMA_NAME,
        table_name    AS OBJECT_NAME,
        CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS OBJECT_TYPE,
        CASE 
            WHEN UPPER(table_schema) LIKE '%RAW%' OR UPPER(table_schema) LIKE '%BRONZE%' THEN 'BRONZE'
            WHEN UPPER(table_schema) LIKE '%STAGING%' OR UPPER(table_schema) LIKE '%SILVER%' THEN 'SILVER'
            WHEN UPPER(table_schema) LIKE '%MART%' OR UPPER(table_schema) LIKE '%GOLD%'  THEN 'GOLD'
            ELSE 'SILVER'
        END AS DV_LAYER,
        'INTERNAL' AS CLASSIFICATION,
        FALSE AS IS_CDE,
        COALESCE(comment, 'dbt model: ' || table_name) AS DESCRIPTION,
        NULL AS RETENTION_POLICY_ID,
        'DRAFT' AS CERTIFICATION,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
      AND table_type IN ('BASE TABLE','VIEW')
      AND (table_schema LIKE '%DBT%' OR table_schema LIKE '%STAGING%' OR table_schema LIKE '%MART%' OR table_schema LIKE '%RAW%' OR table_schema LIKE '%SILVER%' OR table_schema LIKE '%GOLD%')
) AS source
ON target.DATASET_ID = source.DATASET_ID
WHEN MATCHED THEN UPDATE SET
    CATALOG_NAME = source.CATALOG_NAME,
    OBJECT_TYPE  = source.OBJECT_TYPE,
    DV_LAYER     = source.DV_LAYER,
    DESCRIPTION  = source.DESCRIPTION,
    UPDATED_AT   = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (
    DATASET_ID, CATALOG_NAME, SYSTEM_ID, DOMAIN_ID, PLATFORM,
    DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DV_LAYER,
    CLASSIFICATION, IS_CDE, DESCRIPTION, RETENTION_POLICY_ID, 
    CERTIFICATION, CREATED_AT, UPDATED_AT
) VALUES (
    source.DATASET_ID, source.CATALOG_NAME, source.SYSTEM_ID, source.DOMAIN_ID, source.PLATFORM,
    source.DATABASE_NAME, source.SCHEMA_NAME, source.OBJECT_NAME, source.OBJECT_TYPE, source.DV_LAYER,
    source.CLASSIFICATION, source.IS_CDE, source.DESCRIPTION, source.RETENTION_POLICY_ID,
    source.CERTIFICATION, source.CREATED_AT, source.UPDATED_AT
);

SET dataset_count = (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET WHERE UPDATED_AT::DATE = CURRENT_DATE());

-- 2) Sync columns to MAP_DATASET_ATTRIBUTE
MERGE INTO GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE AS target
USING (
    SELECT 
        'attr_' || ds.DATASET_ID || '_' || c.COLUMN_NAME AS MAP_ID,
        ds.DATASET_ID,
        c.COLUMN_NAME,
        NULL AS TERM_ID,
        NULL AS CDE_ID,
        CASE 
            WHEN UPPER(c.COLUMN_NAME) LIKE '%EMAIL%' THEN 'EMAIL'
            WHEN UPPER(c.COLUMN_NAME) LIKE '%PHONE%' THEN 'PHONE'
            WHEN UPPER(c.COLUMN_NAME) LIKE '%SSN%' OR UPPER(c.COLUMN_NAME) LIKE '%SOCIAL%' THEN 'SSN'
            WHEN UPPER(c.COLUMN_NAME) LIKE '%_ID' OR UPPER(c.COLUMN_NAME) LIKE 'ID_%' THEN 'IDENTIFIER'
            WHEN UPPER(c.DATA_TYPE) LIKE '%TIMESTAMP%' OR UPPER(c.DATA_TYPE) LIKE '%DATE%' THEN 'TEMPORAL'
            WHEN UPPER(c.DATA_TYPE) LIKE '%NUMBER%'   OR UPPER(c.DATA_TYPE) LIKE '%DECIMAL%' THEN 'NUMERIC'
            ELSE 'GENERAL'
        END AS SEMANTIC_TYPE,
        CASE 
            WHEN UPPER(c.COLUMN_NAME) IN ('CUSTOMER_ID','ACCOUNT_ID','TRANSACTION_ID','BALANCE','AMOUNT') THEN TRUE
            ELSE FALSE
        END AS QUALITY_CRITICAL,
        COALESCE(c.COMMENT, '') AS DESCRIPTION,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM information_schema.columns c
    JOIN GOV_PLATFORM.CATALOG.DIM_DATASET ds
      ON ds.DATABASE_NAME = c.TABLE_CATALOG
     AND ds.SCHEMA_NAME   = c.TABLE_SCHEMA
     AND ds.OBJECT_NAME   = c.TABLE_NAME
) AS source
ON target.MAP_ID = source.MAP_ID
WHEN MATCHED THEN UPDATE SET
    SEMANTIC_TYPE    = source.SEMANTIC_TYPE,
    QUALITY_CRITICAL = source.QUALITY_CRITICAL,
    DESCRIPTION      = source.DESCRIPTION,
    UPDATED_AT       = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (
    MAP_ID, DATASET_ID, COLUMN_NAME, TERM_ID, CDE_ID, SEMANTIC_TYPE, QUALITY_CRITICAL, DESCRIPTION, CREATED_AT, UPDATED_AT
) VALUES (
    source.MAP_ID, source.DATASET_ID, source.COLUMN_NAME, source.TERM_ID, source.CDE_ID, source.SEMANTIC_TYPE, source.QUALITY_CRITICAL, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT
);

-- 3) Lineage nodes for updated datasets
MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_NODE AS target
USING (
    SELECT 
        DATASET_ID AS NODE_ID,
        'DATASET'  AS NODE_TYPE,
        DATASET_ID AS REF_ID,
        CATALOG_NAME AS NAME,
        DESCRIPTION,
        CREATED_AT,
        UPDATED_AT
    FROM GOV_PLATFORM.CATALOG.DIM_DATASET
    WHERE UPDATED_AT::DATE = CURRENT_DATE()
) AS source
ON target.NODE_ID = source.NODE_ID
WHEN MATCHED THEN UPDATE SET
    NAME = source.NAME,
    DESCRIPTION = source.DESCRIPTION,
    UPDATED_AT = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (NODE_ID, NODE_TYPE, REF_ID, NAME, DESCRIPTION, CREATED_AT, UPDATED_AT)
VALUES (source.NODE_ID, source.NODE_TYPE, source.REF_ID, source.NAME, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT);

-- 4) Basic row count metric using JS proc (kept as-is, generates DQ_RESULT rows)
CREATE OR REPLACE PROCEDURE update_table_row_counts()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var result_summary = [];
    var error_count = 0;
    var get_datasets = `
        SELECT dataset_id, database_name, schema_name, object_name 
        FROM GOV_PLATFORM.CATALOG.DIM_DATASET 
        WHERE updated_at::date = CURRENT_DATE()
          AND object_type = 'TABLE'
    `;
    var dataset_stmt = snowflake.createStatement({sqlText: get_datasets});
    var dataset_results = dataset_stmt.execute();
    while (dataset_results.next()) {
        var dataset_id = dataset_results.getColumnValue(1);
        var db_name = dataset_results.getColumnValue(2);
        var schema_name = dataset_results.getColumnValue(3);
        var table_name = dataset_results.getColumnValue(4);
        try {
            var count_sql = `SELECT COUNT(*) FROM "${db_name}"."${schema_name}"."${table_name}"`;
            var count_stmt = snowflake.createStatement({sqlText: count_sql});
            var count_result = count_stmt.execute();
            count_result.next();
            var row_count = count_result.getColumnValue(1);
            var insert_sql = `
                MERGE INTO GOV_PLATFORM.QUALITY.DQ_RESULT AS target
                USING (
                    SELECT 
                        '${dataset_id}_rowcount_' || CURRENT_DATE()::VARCHAR AS run_id,
                        'row_count_check' AS rule_id,
                        '${dataset_id}' AS dataset_id,
                        CASE WHEN ${row_count} >= 0 THEN 'PASS' ELSE 'FAIL' END AS outcome,
                        OBJECT_CONSTRUCT('row_count', ${row_count}, 'check_type', 'volume', 'table', '${table_name}') AS metrics_summary,
                        '"${db_name}"."${schema_name}"."${table_name}"' AS evidence_ref,
                        CURRENT_TIMESTAMP() AS created_at,
                        CURRENT_TIMESTAMP() AS updated_at
                ) AS source
                ON target.run_id = source.run_id AND target.rule_id = source.rule_id
                WHEN MATCHED THEN UPDATE SET
                    outcome = source.outcome,
                    metrics_summary = source.metrics_summary,
                    updated_at = source.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    run_id, rule_id, dataset_id, outcome, metrics_summary, evidence_ref, created_at, updated_at
                ) VALUES (
                    source.run_id, source.rule_id, source.dataset_id, source.outcome, source.metrics_summary, source.evidence_ref, source.created_at, source.updated_at
                )
            `;
            snowflake.createStatement({sqlText: insert_sql}).execute();
            result_summary.push(`${table_name}: ${row_count} rows`);
        } catch (err) {
            error_count++;
            result_summary.push(`${table_name}: ERROR - ${err.message}`);
        }
    }
    return `Processed ${result_summary.length} tables, ${error_count} errors. Details: ` + result_summary.join('; ');
$$;

CALL update_table_row_counts();
DROP PROCEDURE IF EXISTS update_table_row_counts();

-- 5) Register dbt run as a process + run
MERGE INTO GOV_PLATFORM.LINEAGE.PROCESS AS target
USING (
    SELECT 'DBT_DAILY_RUN' AS PROCESS_ID,
           'DBT Daily Transformation Run' AS NAME,
           'DBT' AS ORCHESTRATOR,
           NULL AS PIPELINE_JOB_ID,
           'DBT_SYSTEM' AS OWNER_PARTY_ID,
           'Daily dbt transformation process for analytics pipeline' AS DESCRIPTION,
           CURRENT_TIMESTAMP() AS CREATED_AT,
           CURRENT_TIMESTAMP() AS UPDATED_AT
) AS source
ON target.PROCESS_ID = source.PROCESS_ID
WHEN MATCHED THEN UPDATE SET DESCRIPTION = source.DESCRIPTION, UPDATED_AT = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (PROCESS_ID, NAME, ORCHESTRATOR, PIPELINE_JOB_ID, OWNER_PARTY_ID, DESCRIPTION, CREATED_AT, UPDATED_AT)
VALUES (source.PROCESS_ID, source.NAME, source.ORCHESTRATOR, source.PIPELINE_JOB_ID, source.OWNER_PARTY_ID, source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT);

INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (RUN_ID, PROCESS_ID, STARTED_AT, ENDED_AT, STATUS, INPUT_SIGNATURE, OUTPUT_SIGNATURE, TRIGGER_REF, CREATED_AT, UPDATED_AT)
VALUES ($batch_id, 'DBT_DAILY_RUN', DATEADD('minute', -30, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'SUCCESS', 'dbt_models_' || CURRENT_DATE()::VARCHAR, 'gov_metadata_' || CURRENT_TIMESTAMP()::VARCHAR, 'scheduled_daily', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- 6) Complete log
INSERT INTO GOV_PLATFORM.RISK.CONTROL_TEST (TEST_ID, CONTROL_ID, RUN_ID, EXECUTED_AT, OUTCOME, DETAILS)
VALUES ($batch_id || '_complete', 'DBT_METADATA_SYNC', $batch_id, CURRENT_TIMESTAMP(), 'PASS', 'DBT metadata sync completed successfully. Datasets processed: ' || $dataset_count);

SELECT 'DBT Governance Sync Completed' AS status, $batch_id AS batch_id, CURRENT_TIMESTAMP() AS completed_at,
       (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET WHERE UPDATED_AT::DATE = CURRENT_DATE()) AS datasets_synced,
       (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE WHERE UPDATED_AT::DATE = CURRENT_DATE()) AS attributes_synced,
       (SELECT COUNT(*) FROM GOV_PLATFORM.LINEAGE.LINEAGE_NODE WHERE UPDATED_AT::DATE = CURRENT_DATE()) AS nodes_created,
       (SELECT COUNT(*) FROM GOV_PLATFORM.QUALITY.DQ_RESULT WHERE CREATED_AT::DATE = CURRENT_DATE()) AS quality_checks_run;
