# Deployment Guide - Snowflake Governance Platform

This guide provides step-by-step instructions for deploying the Snowflake Governance Platform based on the latest code updates.

## 📋 Prerequisites

- Snowflake account with ACCOUNTADMIN privileges
- Access to create databases, schemas, and Streamlit applications
- SnowSQL CLI installed and configured
- Git repository access to the platform code

## 🚀 Deployment Steps

### Step 1: Core Infrastructure Deployment

Deploy the foundational database structures and security model:

```bash
# Execute the unified database setup script
snowsql -f database_setup/unified_setup.sql

# Expected output:
# - GOV_PLATFORM and GOV_APP databases created
# - All required schemas established
# - Complete table structure with foreign keys
# - Basic role grants configured
```

**Key Components Created:**
- **Databases**: `GOV_PLATFORM`, `GOV_APP`
- **Schemas**: All governance, catalog, lineage, quality, risk, and application schemas
- **Tables**: Complete table structure with relationships and constraints
- **Grants**: Basic permissions for `GOVERNANCE_ADMIN` role

### Step 2: Application Layer Setup

Deploy the unified application layer with security policies and views:

```bash
# Deploy unified application layer
snowsql -f app_layer/unified_view.sql

# This script creates:
# - Row access policies with domain and classification filtering
# - Masking policies for email, sensitive data, and evidence references
# - Secure views for all application components
# - Configuration tables for feature flags and access control
```

**Key Components Created:**
- **Security Policies**: `ROW_ACCESS_GOVERNANCE`, `MASK_EMAIL`, `MASK_SENSITIVE_SAMPLES`
- **Secure Views**: `VW_TODAY_HEALTH`, `VW_DQ_RESULTS_ENRICHED`, `VW_RISK_DASHBOARD`, etc.
- **Config Tables**: `APP_FEATURE_FLAG`, `ROLE_PAGE_ACCESS`, `ACCESS_LOG`

### Step 3: Streamlit Application Bootstrap

Set up the Streamlit application infrastructure:

```bash
# Create application infrastructure
snowsql -f deployment/app_build.sql

# This creates:
# - GOV_APP.APP schema and stage
# - GOVERNANCE_APP Streamlit object
# - Required grants and permissions
```

### Step 4: Upload Application Files

Upload the Streamlit application code to the Snowflake stage:

```bash
# Upload all streamlit_app files to the stage
snowsql -c <your_connection> -q "PUT file://streamlit_app/* @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Alternative: Upload individual directories
snowsql -c <your_connection> -q "PUT file://streamlit_app/Home.py @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snowsql -c <your_connection> -q "PUT file://streamlit_app/lib/*.py @GOV_APP.APP.APP_STAGE/lib AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
snowsql -c <your_connection> -q "PUT file://streamlit_app/requirements.txt @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
```

### Step 5: Refresh and Activate Application

Refresh the stage and Streamlit application to recognize the new files:

```sql
-- Refresh stage contents
ALTER STAGE GOV_APP.APP.APP_STAGE REFRESH;

-- Refresh Streamlit application
ALTER STREAMLIT GOV_APP.APP.GOVERNANCE_APP REFRESH;

-- Verify the application is ready
SHOW STREAMLITS IN SCHEMA GOV_APP.APP;
```

### Step 6: Load Sample Data

Load sample data for testing and validation:

```bash
# Load sample data
snowsql -f sample_data/01_sample_data_generation.sql
```

### Step 7: Validation

Validate the deployment by testing key components:

```sql
-- Test secure views
SELECT COUNT(*) FROM GOV_APP.VIEWS.VW_TODAY_HEALTH;
SELECT COUNT(*) FROM GOV_APP.VIEWS.VW_DQ_RESULTS_ENRICHED;
SELECT COUNT(*) FROM GOV_APP.VIEWS.VW_RISK_DASHBOARD;

-- Check feature flags
SELECT * FROM GOV_APP.CONFIG.APP_FEATURE_FLAG;

-- Verify access control
SELECT * FROM GOV_APP.CONFIG.ROLE_PAGE_ACCESS;

-- Test application access
-- Navigate to the Streamlit app URL provided by Snowflake
```

## 🔄 Updates and Maintenance

### Updating Application Code

When updating the Streamlit application:

```bash
# Re-upload updated files
snowsql -c <your_connection> -q "PUT file://streamlit_app/* @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

# Refresh the application
snowsql -c <your_connection> -q "ALTER STAGE GOV_APP.APP.APP_STAGE REFRESH"
snowsql -c <your_connection> -q "ALTER STREAMLIT GOV_APP.APP.GOVERNANCE_APP REFRESH"
```

### Updating Security Policies

To update security policies without recreating views:

```sql
-- Update row access policy
CREATE OR REPLACE ROW ACCESS POLICY GOV_APP.POLICIES.ROW_ACCESS_GOVERNANCE
AS (DOMAIN_NAME STRING, CLASSIFICATION STRING) RETURNS BOOLEAN ->
  -- Updated logic here
  ;

-- Update masking policy
CREATE OR REPLACE MASKING POLICY GOV_APP.POLICIES.MASK_EMAIL 
AS (VAL STRING) RETURNS STRING ->
  -- Updated masking logic here
  ;
```

### Adding New Feature Flags

```sql
-- Add new feature flag
INSERT INTO GOV_APP.CONFIG.APP_FEATURE_FLAG 
  (FEATURE_NAME, ENABLED, DESCRIPTION, ROLES_ALLOWED, CONFIG_JSON)
VALUES 
  ('NEW_FEATURE', TRUE, 'Description of new feature',
   ARRAY_CONSTRUCT('GOVERNANCE_ADMIN','DATA_STEWARD'),
   OBJECT_CONSTRUCT('config_key', 'config_value'));
```

## 🚨 Troubleshooting

### Common Issues

1. **Permission Errors During Deployment**
   ```sql
   -- Ensure you're using ACCOUNTADMIN role
   USE ROLE ACCOUNTADMIN;
   
   -- Check current role
   SELECT CURRENT_ROLE();
   ```

2. **Streamlit App Not Loading**
   ```sql
   -- Check stage contents
   LIST @GOV_APP.APP.APP_STAGE;
   
   -- Verify application status
   SHOW STREAMLITS IN SCHEMA GOV_APP.APP;
   
   -- Check grants
   SHOW GRANTS ON STREAMLIT GOV_APP.APP.GOVERNANCE_APP;
   ```

3. **Access Denied in Application**
   ```sql
   -- Check role page access
   SELECT * FROM GOV_APP.CONFIG.ROLE_PAGE_ACCESS WHERE UPPER(ROLE_NAME) = UPPER(CURRENT_ROLE());
   
   -- Check access log
   SELECT * FROM GOV_APP.CONFIG.ACCESS_LOG ORDER BY TS DESC LIMIT 10;
   ```

4. **Views Returning No Data**
   ```sql
   -- Check row access policies
   DESCRIBE POLICY GOV_APP.POLICIES.ROW_ACCESS_GOVERNANCE;
   
   -- Test without security policies (as admin)
   SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET;
   ```

### Database Connection Issues

If you encounter connection issues:

```bash
# Test connection
snowsql -c <your_connection> -q "SELECT CURRENT_VERSION()"

# Check connection parameters
snowsql -c <your_connection> -o friendly=false -o header=false -q "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
```

## 📈 Performance Optimization

### Post-Deployment Tuning

1. **Warehouse Sizing**
   ```sql
   -- Monitor warehouse usage
   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
   WHERE WAREHOUSE_NAME = 'GOVERNANCE_APP_WH'
   ORDER BY START_TIME DESC;
   
   -- Adjust size if needed
   ALTER WAREHOUSE GOVERNANCE_APP_WH SET WAREHOUSE_SIZE = 'SMALL';
   ```

2. **Query Performance**
   ```sql
   -- Monitor query performance
   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
   WHERE WAREHOUSE_NAME = 'GOVERNANCE_APP_WH'
   ORDER BY START_TIME DESC;
   ```

3. **View Optimization**
   ```sql
   -- Add clustering keys to frequently queried tables
   ALTER TABLE GOV_PLATFORM.CATALOG.DIM_DATASET 
   CLUSTER BY (DOMAIN_ID, CLASSIFICATION);
   ```

## 📝 Deployment Checklist

- [ ] Core infrastructure deployed (`database_setup/unified_setup.sql`)
- [ ] Application layer configured (`app_layer/unified_view.sql`)
- [ ] Streamlit infrastructure created (`deployment/app_build.sql`)
- [ ] Application files uploaded to stage
- [ ] Stage and application refreshed
- [ ] Sample data loaded
- [ ] Security policies validated
- [ ] Feature flags configured
- [ ] Access control tested
- [ ] Application accessible via Snowflake UI
- [ ] Performance monitoring enabled

## 🔗 Next Steps

After successful deployment:

1. **Configure dbt Integration**: Follow the dbt integration guide in `dbt_integration/README.md`
2. **Set Up Monitoring**: Implement monitoring for query performance and user activity
3. **User Training**: Train end users on the governance platform features
4. **Data Integration**: Begin connecting your data sources and implementing governance workflows

For additional support, refer to the main README.md or contact the platform administrators.
