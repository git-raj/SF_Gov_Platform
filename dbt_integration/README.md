# SF Gov Platform - DBT Integration

This directory contains tools and scripts to integrate dbt ETL processes with the SF Gov Platform governance database built on Snowflake.

## Overview

The SF Gov Platform dbt integration automatically captures metadata from dbt runs and pushes it to the comprehensive governance database schemas. This integration provides:

- **Dataset Registration**: Automatic registration of dbt models in `GOV_PLATFORM.CATALOG.DIM_DATASET`
- **Column-Level Metadata**: Mapping of columns to `GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE` with semantic type detection
- **Lineage Tracking**: Creation of lineage nodes and edges in `GOV_PLATFORM.LINEAGE` schema
- **Quality Metrics**: Recording of row counts and basic quality checks in `GOV_PLATFORM.QUALITY.DQ_RESULT`
- **Governance Integration**: Integration with existing classification, policy, and control frameworks

## Architecture Integration

### Governance Database Schemas

The integration maps dbt metadata to the following SF Gov Platform schemas:

- **CATALOG Schema**: `DIM_DATASET`, `MAP_DATASET_ATTRIBUTE`, `DIM_DOMAIN`, `DIM_SYSTEM`
- **LINEAGE Schema**: `LINEAGE_NODE`, `LINEAGE_EDGE`, `PROCESS`, `PROCESS_RUN`
- **QUALITY Schema**: `DQ_RULE`, `DQ_RESULT`, `DQ_RUN`
- **GOVERNANCE Schema**: `CLASSIFICATION`, `POLICY_REGISTRY`, `CONTROL_REGISTRY`
- **OWNERSHIP Schema**: `DIM_PARTY` (for system ownership tracking)
- **RISK Schema**: `CONTROL_TEST` (for audit logging)

## Components

### 1. **SF Governance Post-Hook Macro** (`macros/sf_governance_post_hook.sql`)

A dbt macro that runs automatically after each model execution to capture governance metadata:

- Registers models as datasets in the catalog
- Creates lineage nodes and edges for data flow tracking
- Maps columns with intelligent semantic type detection
- Records quality metrics (row counts)
- Integrates with existing governance classifications

### 2. **Setup Macro** (`macros/sf_governance_setup.sql`)

Ensures all required reference data exists:
- Default systems, domains, and parties
- Classification vocabularies (INTERNAL, CONFIDENTIAL, PII, PUBLIC)
- Basic data quality rules
- Proper dimensional data setup

### 3. **DBT Project Configuration** (`dbt_project.yml`)

Sample configuration that enables governance integration:
- Configures post-hooks for all models
- Sets up governance variables and metadata
- Enables automatic reference data setup

### 4. **Post-Run SQL Script** (`post_run_sf_governance.sql`)

Comprehensive batch sync script for complete metadata synchronization:
- Bulk dataset and column synchronization
- Dynamic row count collection using JavaScript procedures
- Process and audit trail creation
- Error handling and logging

## Setup Instructions

### Prerequisites

1. **SF Gov Platform Database**: Ensure the governance database schemas are deployed
2. **Snowflake Access**: Appropriate permissions to read/write governance tables
3. **DBT Installation**: dbt-snowflake adapter configured
4. **Warehouse Access**: Access to `GOVERNANCE_WH` or similar compute resource

### Method 1: Real-Time Integration (Recommended)

1. **Copy macros to your dbt project:**
   ```bash
   cp macros/*.sql your_dbt_project/macros/
   ```

2. **Update your `dbt_project.yml`:**
   ```yaml
   models:
     your_project:
       +post-hook: "{{ sf_governance_post_hook() }}"
   
   on-run-start:
     - "{{ sf_governance_setup() }}"
   
   vars:
     governance_enabled: true
     lineage_tracking: true
   ```

3. **Configure model metadata (optional):**
   ```sql
   -- models/your_model.sql
   {{ config(
       meta={
           "classification": "CONFIDENTIAL",
           "contains_pii": true,
           "quality_critical_columns": ["customer_id", "account_id"]
       }
   ) }}
   ```

4. **Run dbt as usual:**
   ```bash
   dbt run --target prod
   ```

### Method 2: Batch Processing

1. **Run the post-run script after dbt:**
   ```bash
   # Run dbt
   dbt run --target prod
   
   # Sync governance metadata
   snowsql -f post_run_sf_governance.sql -D run_date=$(date +%Y-%m-%d)
   ```

2. **Schedule in your orchestrator:**
   ```yaml
   # Airflow example
   dbt_run = BashOperator(
       task_id='dbt_run',
       bash_command='dbt run --target prod'
   )
   
   governance_sync = SnowflakeOperator(
       task_id='governance_sync',
       sql='post_run_sf_governance.sql',
       snowflake_conn_id='snowflake_default'
   )
   
   dbt_run >> governance_sync
   ```

## Configuration Options

### Model-Level Metadata

Configure governance metadata directly in your models:

```sql
{{ config(
    materialized='table',
    meta={
        "classification": "PII",           -- Data classification
        "contains_pii": true,              -- PII flag
        "quality_critical_columns": [      -- Important columns for DQ
            "customer_id", 
            "account_balance", 
            "transaction_date"
        ],
        "domain": "BANKING",               -- Business domain override
        "retention_days": 2555             -- Data retention period
    }
) }}
```

### Project-Level Variables

Set governance defaults in `dbt_project.yml`:

```yaml
vars:
  # Governance settings
  governance_enabled: true
  lineage_tracking: true
  auto_classification: true
  
  # Default metadata
  default_domain: "ANALYTICS"
  default_classification: "INTERNAL"
  
  # Quality settings
  enable_row_count_checks: true
  min_row_threshold: 0
```

## Features

### Intelligent Semantic Detection

The integration automatically detects semantic types for columns:

- **EMAIL**: Columns containing "email" in the name
- **PHONE**: Columns containing "phone" in the name
- **SSN**: Columns containing "ssn" or "social" in the name
- **IDENTIFIER**: Columns ending with "_id" or starting with "id_"
- **TEMPORAL**: DATE, TIMESTAMP, or TIME data types
- **NUMERIC**: NUMBER, DECIMAL, or numeric data types

### Data Layer Classification

Automatically classifies models based on schema naming conventions:

- **BRONZE**: Raw or bronze schemas (raw data ingestion layer)
- **SILVER**: Staging or silver schemas (cleaned and transformed data)
- **GOLD**: Marts or gold schemas (business-ready analytics data)

### Quality Critical Flagging

Automatically flags common business-critical columns:
- `CUSTOMER_ID`, `ACCOUNT_ID`, `TRANSACTION_ID`
- `BALANCE`, `AMOUNT`
- Any columns specified in model `meta.quality_critical_columns`

## Monitoring and Validation

### Governance Dashboard Queries

Check sync results:

```sql
-- Recent dataset registrations
SELECT dataset_id, catalog_name, dv_layer, classification, updated_at
FROM GOV_PLATFORM.CATALOG.DIM_DATASET 
WHERE updated_at >= CURRENT_DATE() - 1
ORDER BY updated_at DESC;

-- Quality metrics summary
SELECT 
    ds.catalog_name,
    qr.outcome,
    qr.metrics_summary:row_count::NUMBER as row_count,
    qr.created_at
FROM GOV_PLATFORM.QUALITY.DQ_RESULT qr
JOIN GOV_PLATFORM.CATALOG.DIM_DATASET ds ON qr.dataset_id = ds.dataset_id
WHERE qr.created_at >= CURRENT_DATE() - 1;

-- Lineage coverage
SELECT 
    COUNT(*) as total_nodes,
    COUNT(CASE WHEN node_type = 'DATASET' THEN 1 END) as dataset_nodes
FROM GOV_PLATFORM.LINEAGE.LINEAGE_NODE
WHERE updated_at >= CURRENT_DATE() - 1;
```

### Audit Trail

Monitor governance sync operations:

```sql
-- Control test results (audit logs)
SELECT test_id, outcome, details, executed_at
FROM GOV_PLATFORM.RISK.CONTROL_TEST
WHERE control_id = 'DBT_METADATA_SYNC'
ORDER BY executed_at DESC
LIMIT 10;
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   ```
   Error: Insufficient privileges to operate on database 'GOV_PLATFORM'
   ```
   **Solution**: Ensure your dbt user has `USAGE` on `GOV_PLATFORM` database and `INSERT`/`UPDATE` privileges on governance schemas.

2. **Missing Reference Data**
   ```
   Error: Foreign key constraint violation on DIM_SYSTEM
   ```
   **Solution**: Run the setup macro manually: `dbt run-operation sf_governance_setup`

3. **Schema Not Found**
   ```
   Error: Schema 'GOV_PLATFORM.CATALOG' does not exist
   ```
   **Solution**: Deploy the SF Gov Platform database schemas first using the setup scripts.

### Debug Mode

Enable detailed logging in your dbt runs:

```bash
dbt run --debug --target prod
```

Check governance logs:
```sql
SELECT * FROM GOV_PLATFORM.RISK.CONTROL_TEST 
WHERE control_id = 'DBT_METADATA_SYNC' 
AND executed_at >= CURRENT_DATE() - 1;
```

## Integration Examples

### CI/CD Pipeline Integration

```yaml
# GitHub Actions example
name: DBT with Governance
jobs:
  dbt-run:
    steps:
      - name: Run DBT
        run: dbt run --target prod
        
      - name: Sync Governance Metadata
        run: |
          snowsql -f dbt_integration/post_run_sf_governance.sql \
                  -D run_date=$(date +%Y-%m-%d) \
                  -D batch_id="github_${{ github.run_id }}"
```

### Custom Post-Hook Example

For specific models requiring additional governance metadata:

```sql
-- models/customer_pii_data.sql
{{ config(
    post_hook=[
        "{{ sf_governance_post_hook() }}",
        "UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET 
         SET classification = 'PII', is_cde = TRUE 
         WHERE dataset_id = 'dbt_{{ this.database }}_{{ this.schema }}_{{ this.identifier }}'"
    ]
) }}
```

## Performance Considerations

- **Post-Hook Performance**: Each model execution runs governance sync - consider disabling for development
- **Batch Processing**: For large dbt projects, batch processing may be more efficient
- **Column Profiling**: Disabled by default as it can be resource-intensive
- **Warehouse Usage**: Governance operations use the configured Snowflake warehouse

## Support and Maintenance

### Health Checks

Run periodic validation:

```sql
-- Data freshness check
SELECT 
    'Catalog Coverage' as metric,
    COUNT(*) as value
FROM GOV_PLATFORM.CATALOG.DIM_DATASET 
WHERE updated_at >= CURRENT_DATE() - 7;

-- Quality metrics coverage
SELECT 
    'Quality Checks' as metric,
    COUNT(DISTINCT dataset_id) as datasets_with_metrics
FROM GOV_PLATFORM.QUALITY.DQ_RESULT 
WHERE created_at >= CURRENT_DATE() - 1;
```

### Maintenance Tasks

- Monitor storage growth in governance schemas
- Archive old quality results and audit logs
- Update classification rules and semantic detection patterns
- Review and update critical data element mappings

For additional support or custom configurations, refer to the SF Gov Platform documentation or contact the Data Engineering team.
