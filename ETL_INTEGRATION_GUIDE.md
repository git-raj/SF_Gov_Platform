# ETL Integration Guide - Snowflake Governance Platform

This guide provides comprehensive instructions for integrating various ETL tools with the Snowflake Governance Platform to ensure proper metadata capture, lineage tracking, and governance coverage.

## 📋 Overview

The Snowflake Governance Platform supports integration with multiple ETL tools to automatically capture:
- **Dataset Registration**: Automatic cataloging of all ETL outputs
- **Column-Level Metadata**: Semantic classification and data profiling
- **Lineage Tracking**: End-to-end data flow visualization
- **Quality Metrics**: Automated data quality assessment
- **Governance Compliance**: CDMC framework alignment

## 🔧 Supported ETL Tools

### 1. dbt (Data Build Tool) - Primary Integration

#### Quick Start
```yaml
# dbt_project.yml
models:
  your_project:
    +post-hook: "{{ sf_governance_post_hook() }}"

on-run-start:
  - "{{ sf_governance_setup() }}"

vars:
  governance_enabled: true
  lineage_tracking: true
```

#### Advanced Configuration
```sql
-- models/your_model.sql
{{ config(
    materialized='table',
    meta={
        "classification": "PII",
        "domain": "CUSTOMER",
        "contains_pii": true,
        "quality_critical_columns": ["customer_id", "email", "phone"],
        "retention_days": 2555,
        "data_contract": true
    }
) }}

SELECT 
    customer_id,
    email,
    phone,
    created_date
FROM {{ ref('staging_customers') }}
```

#### Metadata Captured
- **Dataset Registration**: All models automatically registered in `DIM_DATASET`
- **Column Classification**: Intelligent semantic type detection
- **Data Layer Detection**: Bronze/Silver/Gold based on schema naming
- **Quality Metrics**: Row count validation, completeness checks
- **Lineage**: Model dependencies and transformations

#### Quality Checks Integration
```sql
-- schema.yml
version: 2

models:
  - name: customer_data
    tests:
      - dbt_utils.row_count:
          severity: error
      - not_null:
          column_name: customer_id
          severity: error
    columns:
      - name: email
        tests:
          - unique
          - not_null
```

### 2. Apache Airflow Integration

#### DAG Configuration
```python
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

def create_governance_dag():
    dag = DAG(
        'governance_etl_integration',
        default_args={
            'owner': 'data-engineering',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        schedule_interval='@daily',
        catchup=False
    )
    
    # Pre-ETL governance setup
    setup_governance = SnowflakeOperator(
        task_id='setup_governance',
        sql="""
        CALL GOV_PLATFORM.OPERATIONS.REGISTER_PROCESS(
            'airflow_{{ ds }}',
            'AIRFLOW_ETL',
            '{{ dag.dag_id }}',
            'STARTED'
        );
        """,
        snowflake_conn_id='snowflake_default',
        dag=dag
    )
    
    # Main ETL task
    etl_task = SnowflakeOperator(
        task_id='main_etl',
        sql="""
        -- Your ETL logic here
        INSERT INTO target_table SELECT * FROM source_table;
        
        -- Register dataset
        CALL GOV_PLATFORM.OPERATIONS.REGISTER_DATASET(
            'airflow_{{ ds }}_target_table',
            'AIRFLOW_SYSTEM',
            'ANALYTICS',
            '{{ params.target_database }}',
            '{{ params.target_schema }}',
            'target_table'
        );
        """,
        params={
            'target_database': 'ANALYTICS_DB',
            'target_schema': 'MART'
        },
        snowflake_conn_id='snowflake_default',
        dag=dag
    )
    
    # Post-ETL governance validation
    validate_governance = SnowflakeOperator(
        task_id='validate_governance',
        sql="""
        CALL GOV_PLATFORM.OPERATIONS.VALIDATE_ETL_RUN(
            'airflow_{{ ds }}',
            'target_table',
            {{ task_instance.xcom_pull(task_ids='main_etl') }}
        );
        """,
        snowflake_conn_id='snowflake_default',
        dag=dag
    )
    
    setup_governance >> etl_task >> validate_governance
    return dag

governance_dag = create_governance_dag()
```

#### Governance Procedures for Airflow
```sql
-- Create in GOV_PLATFORM.OPERATIONS schema
CREATE OR REPLACE PROCEDURE REGISTER_PROCESS(
    process_id STRING,
    system_id STRING,
    process_name STRING,
    status STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (
        RUN_ID, PROCESS_ID, SYSTEM_ID, STATUS, STARTED_AT, CREATED_AT, UPDATED_AT
    ) VALUES (
        process_id,
        process_name,
        system_id,
        status,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
    RETURN 'Process registered: ' || process_id;
END;
$$;

CREATE OR REPLACE PROCEDURE REGISTER_DATASET(
    dataset_id STRING,
    system_id STRING,
    domain_id STRING,
    database_name STRING,
    schema_name STRING,
    table_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_DATASET AS target
    USING (
        SELECT 
            dataset_id AS DATASET_ID,
            database_name || '.' || schema_name || '.' || table_name AS CATALOG_NAME,
            system_id AS SYSTEM_ID,
            domain_id AS DOMAIN_ID,
            'SNOWFLAKE' AS PLATFORM,
            database_name AS DATABASE_NAME,
            schema_name AS SCHEMA_NAME,
            table_name AS OBJECT_NAME,
            'TABLE' AS OBJECT_TYPE,
            CASE 
                WHEN UPPER(schema_name) LIKE '%RAW%' THEN 'BRONZE'
                WHEN UPPER(schema_name) LIKE '%STAGING%' THEN 'SILVER'
                WHEN UPPER(schema_name) LIKE '%MART%' THEN 'GOLD'
                ELSE 'SILVER'
            END AS DV_LAYER,
            'INTERNAL' AS CLASSIFICATION,
            FALSE AS IS_CDE,
            'Airflow generated dataset' AS DESCRIPTION,
            NULL AS RETENTION_POLICY_ID,
            'DRAFT' AS CERTIFICATION,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.DATASET_ID = source.DATASET_ID
    WHEN MATCHED THEN UPDATE SET
        CATALOG_NAME = source.CATALOG_NAME,
        UPDATED_AT = source.UPDATED_AT
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
    RETURN 'Dataset registered: ' || dataset_id;
END;
$$;
```

### 3. Informatica PowerCenter/IICS Integration

#### Mapping Configuration
```sql
-- Post-session SQL for Informatica mappings
-- Execute in target connection after each mapping

-- Register the mapping execution
INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (
    RUN_ID, PROCESS_ID, SYSTEM_ID, STATUS, STARTED_AT, ENDED_AT, CREATED_AT, UPDATED_AT
) VALUES (
    'informatica_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'),
    '$PMSessionName',  -- Informatica parameter
    'INFORMATICA_SYSTEM',
    CASE WHEN $PMSessionStatusCode = 0 THEN 'SUCCESS' ELSE 'FAILED' END,
    CURRENT_TIMESTAMP() - INTERVAL '$PMPrevSessionRunTime minutes',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- Register target datasets
CALL GOV_PLATFORM.OPERATIONS.REGISTER_DATASET(
    'informatica_$PMSessionName_$TgtTableName',
    'INFORMATICA_SYSTEM',
    '$DomainName',  -- Custom parameter
    '$TgtDatabaseName',
    '$TgtOwnerName',
    '$TgtTableName'
);
```

#### Quality Validation
```sql
-- Create quality check procedure for Informatica
CREATE OR REPLACE PROCEDURE INFORMATICA_QUALITY_CHECK(
    session_name STRING,
    target_table STRING,
    source_count NUMBER,
    target_count NUMBER,
    error_count NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO GOV_PLATFORM.QUALITY.DQ_RESULT (
        RUN_ID, RULE_ID, DATASET_ID, OUTCOME, METRICS_SUMMARY, EVIDENCE_REF, CREATED_AT, UPDATED_AT
    ) VALUES (
        'informatica_' || session_name || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'),
        'row_count_validation',
        'informatica_' || session_name || '_' || target_table,
        CASE 
            WHEN target_count = source_count - error_count THEN 'PASS'
            WHEN error_count < (source_count * 0.05) THEN 'WARN'
            ELSE 'FAIL'
        END,
        OBJECT_CONSTRUCT(
            'source_count', source_count,
            'target_count', target_count,
            'error_count', error_count,
            'success_rate', (target_count * 100.0 / NULLIF(source_count, 0))
        ),
        'informatica_session: ' || session_name,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
    RETURN 'Quality check completed for: ' || target_table;
END;
$$;
```

### 4. Talend Integration

#### Job Configuration
```java
// In Talend job's tJavaRow component
System.out.println("Registering Talend job execution...");

// Create governance registration
tSnowflakeOutput governance_reg = new tSnowflakeOutput();
governance_reg.setProperty("connection", "SNOWFLAKE_GOVERNANCE");
governance_reg.setProperty("sql", 
    "INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN " +
    "(RUN_ID, PROCESS_ID, SYSTEM_ID, STATUS, STARTED_AT, CREATED_AT, UPDATED_AT) " +
    "VALUES ('" + context.jobName + "_" + TalendDate.formatDate("yyyyMMddHHmmss", new Date()) + "', " +
    "'" + context.jobName + "', 'TALEND_SYSTEM', 'STARTED', CURRENT_TIMESTAMP(), " +
    "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
);
```

#### Dataset Registration (tSnowflakeOutput component)
```sql
-- Additional SQL after main job
CALL GOV_PLATFORM.OPERATIONS.REGISTER_DATASET(
    'talend_{{context.jobName}}_{{context.targetTable}}',
    'TALEND_SYSTEM',
    '{{context.domainName}}',
    '{{context.targetDatabase}}',
    '{{context.targetSchema}}',
    '{{context.targetTable}}'
);

-- Create lineage between source and target
INSERT INTO GOV_PLATFORM.LINEAGE.LINEAGE_EDGE (
    EDGE_ID, SRC_NODE_ID, TGT_NODE_ID, EDGE_TYPE, LOGIC_REF, ACTIVE_FLAG, CREATED_AT, UPDATED_AT
) VALUES (
    'talend_edge_{{context.jobName}}_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'),
    'talend_{{context.sourceSystem}}_{{context.sourceTable}}',
    'talend_{{context.jobName}}_{{context.targetTable}}',
    'TRANSFORMS',
    'talend_job: {{context.jobName}}',
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
```

### 5. SSIS Integration

#### Package Configuration
```sql
-- Execute SQL Task in SSIS package (beginning)
DECLARE @ProcessId VARCHAR(100) = 'ssis_' + '$(System::PackageName)' + '_' + FORMAT(GETDATE(), 'yyyyMMddHHmmss')

INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (
    RUN_ID, PROCESS_ID, SYSTEM_ID, STATUS, STARTED_AT, CREATED_AT, UPDATED_AT
) VALUES (
    @ProcessId,
    '$(System::PackageName)',
    'SSIS_SYSTEM',
    'STARTED',
    GETDATE(),
    GETDATE(),
    GETDATE()
)

-- Store process ID in package variable for later use
```

#### Data Flow Task Post-Processing
```sql
-- Execute SQL Task after Data Flow Task
DECLARE @DatasetId VARCHAR(200) = 'ssis_$(System::PackageName)_' + '$(User::TargetTable)'
DECLARE @RowCount INT = ?  -- Parameter from Data Flow Task

EXEC GOV_PLATFORM.OPERATIONS.REGISTER_DATASET 
    @DatasetId,
    'SSIS_SYSTEM',
    '$(User::DomainName)',
    '$(User::TargetDatabase)',
    '$(User::TargetSchema)',
    '$(User::TargetTable)'

-- Quality metrics
INSERT INTO GOV_PLATFORM.QUALITY.DQ_RESULT (
    RUN_ID, RULE_ID, DATASET_ID, OUTCOME, METRICS_SUMMARY, EVIDENCE_REF, CREATED_AT, UPDATED_AT
) VALUES (
    'ssis_' + '$(System::PackageName)' + '_' + FORMAT(GETDATE(), 'yyyyMMddHHmmss'),
    'row_count_check',
    @DatasetId,
    CASE WHEN @RowCount > 0 THEN 'PASS' ELSE 'FAIL' END,
    '{"row_count": ' + CAST(@RowCount AS VARCHAR) + ', "package": "$(System::PackageName)"}',
    'ssis_package: $(System::PackageName)',
    GETDATE(),
    GETDATE()
)
```

## 📊 Generic ETL Integration Pattern

### For Any ETL Tool

#### 1. Pre-Process Registration
```sql
-- Call at start of ETL process
CALL GOV_PLATFORM.OPERATIONS.START_ETL_PROCESS(
    '<unique_process_id>',
    '<etl_tool_name>',
    '<process_name>',
    '<domain_name>'
);
```

#### 2. Dataset Registration
```sql
-- Call for each output dataset
CALL GOV_PLATFORM.OPERATIONS.REGISTER_ETL_OUTPUT(
    '<dataset_id>',
    '<system_id>',
    '<domain_id>',
    '<database_name>',
    '<schema_name>',
    '<table_name>',
    '<classification>',
    '<description>'
);
```

#### 3. Lineage Tracking
```sql
-- Call to establish lineage
CALL GOV_PLATFORM.OPERATIONS.CREATE_LINEAGE(
    '<source_dataset_id>',
    '<target_dataset_id>',
    '<transformation_logic>',
    '<etl_process_name>'
);
```

#### 4. Quality Validation
```sql
-- Call for quality metrics
CALL GOV_PLATFORM.OPERATIONS.RECORD_QUALITY_METRICS(
    '<dataset_id>',
    '<rule_type>',
    '<outcome>',
    '<metrics_json>',
    '<evidence>'
);
```

#### 5. Process Completion
```sql
-- Call at end of ETL process
CALL GOV_PLATFORM.OPERATIONS.COMPLETE_ETL_PROCESS(
    '<process_id>',
    '<status>',
    '<error_message>'
);
```

## 🔧 Universal ETL Procedures

```sql
-- Create universal procedures for ETL integration
CREATE OR REPLACE PROCEDURE GOV_PLATFORM.OPERATIONS.START_ETL_PROCESS(
    process_id STRING,
    system_name STRING,
    process_name STRING,
    domain_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Ensure system exists
    MERGE INTO GOV_PLATFORM.CATALOG.DIM_SYSTEM AS target
    USING (
        SELECT 
            UPPER(system_name) AS SYSTEM_ID,
            system_name AS SYSTEM_NAME,
            'ETL_TOOL' AS SYSTEM_TYPE,
            'DATA_ENGINEERING' AS OWNER_GROUP,
            'ETL system: ' || system_name AS DESCRIPTION,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
    ) AS source
    ON target.SYSTEM_ID = source.SYSTEM_ID
    WHEN NOT MATCHED THEN INSERT (
        SYSTEM_ID, SYSTEM_NAME, SYSTEM_TYPE, OWNER_GROUP, DESCRIPTION, CREATED_AT, UPDATED_AT
    ) VALUES (
        source.SYSTEM_ID, source.SYSTEM_NAME, source.SYSTEM_TYPE, source.OWNER_GROUP, 
        source.DESCRIPTION, source.CREATED_AT, source.UPDATED_AT
    );

    -- Register process run
    INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN (
        RUN_ID, PROCESS_ID, SYSTEM_ID, STATUS, STARTED_AT, CREATED_AT, UPDATED_AT
    ) VALUES (
        process_id,
        process_name,
        UPPER(system_name),
        'STARTED',
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );

    RETURN 'Process started: ' || process_id;
END;
$$;

CREATE OR REPLACE PROCEDURE GOV_PLATFORM.OPERATIONS.COMPLETE_ETL_PROCESS(
    process_id STRING,
    status STRING,
    error_message STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE GOV_PLATFORM.LINEAGE.PROCESS_RUN
    SET 
        STATUS = status,
        ENDED_AT = CURRENT_TIMESTAMP(),
        ERROR_MESSAGE = error_message,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = process_id;

    RETURN 'Process completed: ' || process_id || ' with status: ' || status;
END;
$$;
```

## 📈 Monitoring and Validation

### ETL Health Dashboard
```sql
-- Query for ETL health monitoring
SELECT 
    pr.system_id,
    COUNT(*) AS total_runs,
    COUNT(CASE WHEN pr.status = 'SUCCESS' THEN 1 END) AS successful_runs,
    COUNT(CASE WHEN pr.status = 'FAILED' THEN 1 END) AS failed_runs,
    AVG(DATEDIFF('minute', pr.started_at, pr.ended_at)) AS avg_duration_minutes,
    MAX(pr.started_at) AS last_run_time
FROM GOV_PLATFORM.LINEAGE.PROCESS_RUN pr
WHERE pr.started_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY pr.system_id
ORDER BY pr.system_id;
```

### Data Quality Metrics
```sql
-- ETL quality assessment
SELECT 
    ds.system_id,
    ds.domain_id,
    COUNT(DISTINCT ds.dataset_id) AS total_datasets,
    COUNT(dq.rule_id) AS total_quality_checks,
    COUNT(CASE WHEN dq.outcome = 'PASS' THEN 1 END) AS passed_checks,
    ROUND(COUNT(CASE WHEN dq.outcome = 'PASS' THEN 1 END) * 100.0 / NULLIF(COUNT(dq.rule_id), 0), 2) AS pass_rate_pct
FROM GOV_PLATFORM.CATALOG.DIM_DATASET ds
LEFT JOIN GOV_PLATFORM.QUALITY.DQ_RESULT dq ON dq.dataset_id = ds.dataset_id
WHERE dq.created_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY ds.system_id, ds.domain_id
ORDER BY ds.system_id, ds.domain_id;
```

## 🚨 Best Practices

### 1. Error Handling
- Always wrap governance calls in try-catch blocks
- Log governance failures without failing the main ETL job
- Implement retry logic for governance API calls

### 2. Performance Optimization
- Batch governance operations when possible
- Use asynchronous calls for non-critical metadata
- Implement caching for frequently accessed reference data

### 3. Security Considerations
- Use service accounts with minimal required permissions
- Encrypt sensitive metadata in transit and at rest
- Implement audit trails for all governance operations

### 4. Testing and Validation
- Test governance integration in development environments
- Validate metadata accuracy regularly
- Monitor governance operation performance

For additional support on ETL integrations, contact the Data Engineering team or refer to the tool-specific documentation in the `dbt_integration/` directory.
