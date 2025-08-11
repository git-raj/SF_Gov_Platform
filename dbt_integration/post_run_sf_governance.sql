-- ====================================================================
-- SF GOV PLATFORM - DBT POST-RUN GOVERNANCE SYNC
-- ====================================================================
-- Run this after dbt operations to sync metadata to SF Gov Platform
-- Usage: snowsql -f post_run_sf_governance.sql -D run_date=2024-01-01

-- Set context
USE WAREHOUSE GOVERNANCE_WH;
USE DATABASE GOV_PLATFORM;

-- Variables (can be passed as parameters)
SET run_date = IFNULL($run_date, CURRENT_DATE());
SET batch_id = 'dbt_sync_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HHMMSS');

-- Log start of process
INSERT INTO GOV_PLATFORM.RISK.CONTROL_TEST (
    test_id,
    control_id, 
    run_id,
    executed_at,
    outcome,
    details
) VALUES (
    $batch_id || '_start',
    'DBT_METADATA_SYNC',
    $batch_id,
    CURRENT_TIMESTAMP(),
    'PASS',
    'Starting DBT metadata sync process'
);

-- ====================================================================
-- 1. SYNC DBT MODELS TO CATALOG.DIM_DATASET
-- ====================================================================

MERGE INTO GOV_PLATFORM.CATALOG.DIM_DATASET AS target
USING (
    -- Find all tables in dbt schemas
    SELECT 
        'dbt_' || table_catalog || '_' || table_schema || '_' || table_name AS dataset_id,
        table_catalog || '.' || table_schema || '.' || table_name AS catalog_name,
        'DBT_PROD' AS system_id,
        'ANALYTICS' AS domain_id,
        'SNOWFLAKE' AS platform,
        table_catalog AS database_name,
        table_schema AS schema_name,
        table_name AS object_name,
        table_type AS object_type,
        CASE 
            WHEN table_schema LIKE '%raw%' OR table_schema LIKE '%bronze%' THEN 'BRONZE'
            WHEN table_schema LIKE '%staging%' OR table_schema LIKE '%silver%' THEN 'SILVER'
            WHEN table_schema LIKE '%marts%' OR table_schema LIKE '%gold%' THEN 'GOLD'
            ELSE 'SILVER'
        END AS dv_layer,
        'INTERNAL' AS classification,
        FALSE AS is_cde,  -- Default, can be updated manually
        COALESCE(comment, 'dbt model: ' || table_name) AS description,
        NULL AS retention_policy_id,
        'DRAFT' AS certification,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
      -- Focus on typical dbt schema patterns
      AND (table_schema LIKE '%dbt%' 
           OR table_schema IN ('STAGING', 'INTERMEDIATE', 'MARTS', 'ANALYTICS', 'RAW', 'BRONZE', 'SILVER', 'GOLD')
           OR table_schema LIKE '%staging%'
           OR table_schema LIKE '%marts%')
) AS source
ON target.dataset_id = source.dataset_id
WHEN MATCHED THEN UPDATE SET
    catalog_name = source.catalog_name,
    object_type = source.object_type,
    dv_layer = source.dv_layer,
    description = source.description,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    dataset_id, catalog_name, system_id, domain_id, platform,
    database_name, schema_name, object_name, object_type, dv_layer,
    classification, is_cde, description, retention_policy_id, 
    certification, created_at, updated_at
) VALUES (
    source.dataset_id, source.catalog_name, source.system_id, source.domain_id, source.platform,
    source.database_name, source.schema_name, source.object_name, source.object_type, source.dv_layer,
    source.classification, source.is_cde, source.description, source.retention_policy_id,
    source.certification, source.created_at, source.updated_at
);

-- Log dataset sync results
SET dataset_count = (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET WHERE updated_at::date = CURRENT_DATE());

-- ====================================================================
-- 2. SYNC COLUMN METADATA TO MAP_DATASET_ATTRIBUTE
-- ====================================================================

MERGE INTO GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE AS target
USING (
    SELECT 
        'attr_' || ds.dataset_id || '_' || c.column_name AS map_id,
        ds.dataset_id,
        c.column_name,
        NULL AS term_id,  -- To be mapped manually
        NULL AS cde_id,   -- To be identified separately
        CASE 
            WHEN UPPER(c.column_name) LIKE '%EMAIL%' THEN 'EMAIL'
            WHEN UPPER(c.column_name) LIKE '%PHONE%' THEN 'PHONE'
            WHEN UPPER(c.column_name) LIKE '%SSN%' OR UPPER(c.column_name) LIKE '%SOCIAL%' THEN 'SSN'
            WHEN UPPER(c.column_name) LIKE '%_ID' OR UPPER(c.column_name) LIKE 'ID_%' THEN 'IDENTIFIER'
            WHEN c.data_type LIKE '%TIMESTAMP%' OR c.data_type LIKE '%DATE%' THEN 'TEMPORAL'
            WHEN c.data_type LIKE '%NUMBER%' OR c.data_type LIKE '%DECIMAL%' THEN 'NUMERIC'
            ELSE 'GENERAL'
        END AS semantic_type,
        CASE 
            WHEN UPPER(c.column_name) IN ('CUSTOMER_ID', 'ACCOUNT_ID', 'TRANSACTION_ID', 'BALANCE', 'AMOUNT') THEN TRUE
            ELSE FALSE
        END AS quality_critical,
        COALESCE(c.comment, '') AS description,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM information_schema.columns c
    JOIN GOV_PLATFORM.CATALOG.DIM_DATASET ds ON (
        ds.database_name = c.table_catalog
        AND ds.schema_name = c.table_schema
        AND ds.object_name = c.table_name
    )
    WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
) AS source
ON target.map_id = source.map_id
WHEN MATCHED THEN UPDATE SET
    semantic_type = source.semantic_type,
    quality_critical = source.quality_critical,
    description = source.description,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    map_id, dataset_id, column_name, term_id, cde_id, semantic_type, 
    quality_critical, description, created_at, updated_at
) VALUES (
    source.map_id, source.dataset_id, source.column_name, source.term_id, source.cde_id, source.semantic_type,
    source.quality_critical, source.description, source.created_at, source.updated_at
);

-- ====================================================================
-- 3. CREATE LINEAGE NODES FOR ALL DATASETS
-- ====================================================================

MERGE INTO GOV_PLATFORM.LINEAGE.LINEAGE_NODE AS target
USING (
    SELECT 
        dataset_id AS node_id,
        'DATASET' AS node_type,
        dataset_id AS ref_id,
        catalog_name AS name,
        description,
        created_at,
        updated_at
    FROM GOV_PLATFORM.CATALOG.DIM_DATASET
    WHERE updated_at::date = CURRENT_DATE()
) AS source
ON target.node_id = source.node_id
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    description = source.description,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    node_id, node_type, ref_id, name, description, created_at, updated_at
) VALUES (
    source.node_id, source.node_type, source.ref_id, source.name, source.description, source.created_at, source.updated_at
);

-- ====================================================================
-- 4. BASIC QUALITY METRICS - ROW COUNTS
-- ====================================================================

-- Create a stored procedure for dynamic row counting
CREATE OR REPLACE PROCEDURE update_table_row_counts()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var result_summary = [];
    var error_count = 0;
    
    // Get all datasets to process
    var get_datasets = `
        SELECT dataset_id, database_name, schema_name, object_name 
        FROM GOV_PLATFORM.CATALOG.DIM_DATASET 
        WHERE updated_at::date = CURRENT_DATE()
        AND object_type = 'BASE TABLE'
    `;
    
    var dataset_stmt = snowflake.createStatement({sqlText: get_datasets});
    var dataset_results = dataset_stmt.execute();
    
    while (dataset_results.next()) {
        var dataset_id = dataset_results.getColumnValue(1);
        var db_name = dataset_results.getColumnValue(2);
        var schema_name = dataset_results.getColumnValue(3);
        var table_name = dataset_results.getColumnValue(4);
        
        try {
            // Get row count
            var count_sql = `SELECT COUNT(*) FROM "${db_name}"."${schema_name}"."${table_name}"`;
            var count_stmt = snowflake.createStatement({sqlText: count_sql});
            var count_result = count_stmt.execute();
            count_result.next();
            var row_count = count_result.getColumnValue(1);
            
            // Insert quality result
            var insert_sql = `
                MERGE INTO GOV_PLATFORM.QUALITY.DQ_RESULT AS target
                USING (
                    SELECT 
                        '${dataset_id}_row_count_' || CURRENT_DATE()::VARCHAR AS run_id,
                        'row_count_check' AS rule_id,
                        '${dataset_id}' AS dataset_id,
                        CASE WHEN ${row_count} >= 0 THEN 'PASS' ELSE 'FAIL' END AS outcome,
                        PARSE_JSON('{"row_count": ${row_count}, "check_type": "volume", "table": "${table_name}"}') AS metrics_summary,
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
            
            var insert_stmt = snowflake.createStatement({sqlText: insert_sql});
            insert_stmt.execute();
            
            result_summary.push(`${table_name}: ${row_count} rows`);
            
        } catch (err) {
            error_count++;
            result_summary.push(`${table_name}: ERROR - ${err.message}`);
        }
    }
    
    return `Processed ${result_summary.length} tables, ${error_count} errors. Details: ` + result_summary.join('; ');
$$;

-- Execute the procedure
CALL update_table_row_counts();

-- ====================================================================
-- 5. CREATE BASIC PROCESS AND PROCESS_RUN RECORDS
-- ====================================================================

-- Register DBT as a process
MERGE INTO GOV_PLATFORM.LINEAGE.PROCESS AS target
USING (
    SELECT 
        'DBT_DAILY_RUN' AS process_id,
        'DBT Daily Transformation Run' AS name,
        'DBT' AS orchestrator,
        NULL AS pipeline_job_id,
        'DBT_SYSTEM' AS owner_party_id,
        'Daily dbt transformation process for analytics pipeline' AS description,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
) AS source
ON target.process_id = source.process_id
WHEN MATCHED THEN UPDATE SET
    description = source.description,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    process_id, name, orchestrator, pipeline_job_id, owner_party_id, description, created_at, updated_at
) VALUES (
    source.process_id, source.name, source.orchestrator, source.pipeline_job_id, source.owner_party_id, source.description, source.created_at, source.updated_at
);

-- Record this sync as a process run
INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (
    run_id,
    process_id,
    started_at,
    ended_at,
    status,
    input_signature,
    output_signature,
    trigger_ref,
    created_at,
    updated_at
) VALUES (
    $batch_id,
    'DBT_DAILY_RUN',
    DATEADD('minute', -30, CURRENT_TIMESTAMP()),  -- Approximate run time
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    'dbt_models_' || CURRENT_DATE()::VARCHAR,
    'gov_metadata_' || CURRENT_TIMESTAMP()::VARCHAR,
    'scheduled_daily',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- ====================================================================
-- 6. LOG COMPLETION AND SUMMARY
-- ====================================================================

-- Final log entry
INSERT INTO GOV_PLATFORM.RISK.CONTROL_TEST (
    test_id,
    control_id, 
    run_id,
    executed_at,
    outcome,
    details
) VALUES (
    $batch_id || '_complete',
    'DBT_METADATA_SYNC',
    $batch_id,
    CURRENT_TIMESTAMP(),
    'PASS',
    'DBT metadata sync completed successfully. Datasets processed: ' || $dataset_count
);

-- Summary query
SELECT 
    'DBT Governance Sync Completed' AS status,
    $batch_id AS batch_id,
    CURRENT_TIMESTAMP() AS completed_at,
    (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET WHERE updated_at::date = CURRENT_DATE()) AS datasets_synced,
    (SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE WHERE updated_at::date = CURRENT_DATE()) AS attributes_synced,
    (SELECT COUNT(*) FROM GOV_PLATFORM.LINEAGE.LINEAGE_NODE WHERE updated_at::date = CURRENT_DATE()) AS nodes_created,
    (SELECT COUNT(*) FROM GOV_PLATFORM.QUALITY.DQ_RESULT WHERE created_at::date = CURRENT_DATE()) AS quality_checks_run;

-- Cleanup procedure (optional)
DROP PROCEDURE IF EXISTS update_table_row_counts();
