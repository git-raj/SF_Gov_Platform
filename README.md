# Snowflake Governance Platform

A comprehensive enterprise-grade data governance platform built natively on Snowflake with Streamlit for visualization and management.

## 🏗️ Architecture Overview

This platform provides end-to-end data governance capabilities including:

- **Data Catalog**: Asset discovery, business glossary, critical data elements (CDEs)
- **Data Quality**: Rule registry, automated testing, results tracking
- **Data Lineage**: Technical and business lineage visualization
- **Risk Management**: Control testing, attestations, risk register
- **Compliance**: Policy management, data contracts, privacy assessments
- **Access Control**: Role-based security, domain segregation, audit trails

### Technology Stack

- **Database**: Snowflake (native data cloud platform)
- **Application**: Streamlit in Snowflake (SiS)
- **Orchestration**: dbt, Airflow, Control-M integration
- **Security**: Native Snowflake RBAC, row-level security, column masking
- **Integration**: APIs for Collibra, JIRA, ServiceNow

## 🚀 Quick Start

### Prerequisites

- Snowflake account with appropriate privileges
- ACCOUNTADMIN or equivalent role for initial setup
- Access to create databases, schemas, and Streamlit apps

### 1. Database Setup

Execute the database setup scripts in order:

```sql
-- 1. Create databases and core schemas
@database_setup/01_governance_database_ddl.sql

-- 2. Create ownership tables
@database_setup/02_ownership_schema.sql

-- 3. Create governance tables
@database_setup/03_governance_schema.sql

-- 4. Create lineage tables
@database_setup/04_lineage_schema.sql

-- 5. Create remaining schemas (security, risk, quality, change)
@database_setup/05_remaining_schemas.sql
```

### 2. Application Layer Setup

```sql
-- Unified application layer setup (views, policies, config)
@app_layer/unified_view.sql
```

### 3. Load Sample Data

```sql
-- Load sample data for testing
@sample_data/01_sample_data_generation.sql
```

### 4. Deploy Streamlit App

1. **Bootstrap the application infrastructure:**
   ```sql
   -- Create schemas, stage, and Streamlit app
   @deployment/app_build.sql
   ```

2. **Upload application files to stage:**
   ```bash
   # Upload streamlit_app/* to @GOV_APP.APP.APP_STAGE
   snowsql -c <connection> -q "PUT file://streamlit_app/* @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
   ```

3. **Alternative deployment script:**
   ```sql
   -- Streamlined deployment option
   @deployment/streamlit_app_deploy.sql
   ```

4. **Refresh the application:**
   ```sql
   ALTER STAGE GOV_APP.APP.APP_STAGE REFRESH;
   ALTER STREAMLIT GOV_APP.APP.GOVERNANCE_APP REFRESH;
   ```

## 📊 Platform Components

### Database Structure

```
GOV_PLATFORM/               -- Core governance database
├── CATALOG/                 -- Data asset catalog
│   ├── DIM_DOMAIN          -- Business domains
│   ├── DIM_SYSTEM          -- Source systems
│   ├── DIM_DATASET         -- Dataset registry
│   ├── DIM_TERM            -- Business glossary
│   ├── DIM_CDE             -- Critical data elements
│   └── MAP_DATASET_ATTRIBUTE -- Column mappings
├── OWNERSHIP/              -- Stewardship and RACI
│   ├── DIM_PARTY           -- People and groups
│   ├── MAP_DATASET_OWNER   -- Dataset ownership
│   └── MAP_RACI            -- RACI matrix
├── GOVERNANCE/             -- Policies and controls
│   ├── POLICY_REGISTRY     -- Policy definitions
│   ├── CONTROL_REGISTRY    -- Control catalog
│   ├── DATA_CONTRACT       -- Data contracts
│   └── CLASSIFICATION      -- Data classifications
├── LINEAGE/                -- Data lineage
│   ├── LINEAGE_NODE        -- Graph nodes
│   ├── LINEAGE_EDGE        -- Graph edges
│   └── PROCESS/PROCESS_RUN -- Process execution
├── SECURITY/               -- Security and privacy
├── RISK/                   -- Risk management
├── QUALITY/                -- Data quality
└── CHANGE/                 -- Change management

GOV_APP/                    -- Application layer
├── VIEWS/                  -- Secure views
├── CONFIG/                 -- App configuration
└── POLICIES/               -- Security policies
```

### Streamlit Application

```
streamlit_app/
├── Home.py                 -- Main dashboard with KPIs and health overview
├── requirements.txt        -- Python dependencies
└── lib/                    -- Utility libraries
    ├── __init__.py         -- Package initialization
    ├── dal.py              -- Data access layer with Snowpark session
    ├── filters.py          -- Filter utilities for domains/systems
    ├── authz.py            -- Role-based authorization with default-allow
    └── charts.py           -- Visualization helpers using Altair
```

## 🔐 Security Model

### Role-Based Access Control

The platform implements a comprehensive RBAC model with role hierarchy:

- **GOVERNANCE_ADMIN**: Full administrative access across all components
- **DATA_STEWARD**: Data stewardship, quality management, and catalog maintenance
- **GOVERNANCE_ANALYST**: Read-only analysis and reporting capabilities
- **AUDIT_ROLE**: Compliance monitoring and audit trail access
- **RISK_MANAGER**: Risk assessment and mitigation management
- **Domain-specific roles**: RETAIL_DATA_ANALYST, LENDING_DATA_ANALYST, etc.

### Advanced Data Protection

- **Row Access Policies**: Combined domain and classification-based filtering
- **Dynamic Masking**: Email masking, evidence reference protection, and sensitive data handling
- **Classification Tiers**: Public, Internal, Confidential, PII, and PCI classifications
- **Secure Views**: All application views implement security policies automatically
- **Access Logging**: Comprehensive audit trail with role-based access attempts

### Security Policy Implementation

```sql
-- Row access policy example
ROW_ACCESS_GOVERNANCE(DOMAIN_NAME, CLASSIFICATION)
-- Masking policies for different data types
MASK_EMAIL, MASK_SENSITIVE_SAMPLES, MASK_EVIDENCE_REF
```

## 📈 Key Features

### Home Dashboard
- **Live KPI Metrics**: Real-time counts of datasets, glossary terms, DQ rules, and open risks
- **Dataset Overview**: Filterable view of all registered datasets with classification and certification status
- **Data Quality Visualization**: Interactive charts showing recent DQ results with validity/completeness rates
- **Risk Register Summary**: Current risk items with severity and status tracking
- **Role-based Access**: Dynamic content based on current user role with comprehensive authorization

### Secure Application Views
- **VW_TODAY_HEALTH**: Real-time health metrics with latest DQ outcomes
- **VW_DQ_RESULTS_ENRICHED**: Comprehensive data quality results with owner information
- **VW_CONTROL_RESULTS_ENRICHED**: Control testing outcomes with audit details
- **VW_DATASET_OWNERS**: Dataset ownership mapping with contact information
- **VW_BUSINESS_GLOSSARY**: Business terms with stewardship details
- **VW_DATA_CONTRACTS**: Producer-consumer agreements with version control
- **VW_RISK_DASHBOARD**: Risk assessment and management overview

### Advanced Authorization
- **Page-Level Access Control**: Configurable access rules per role and page
- **Default-Allow Security**: Graceful degradation when access rules are not configured
- **Audit Trail**: Complete logging of all access attempts and outcomes
- **Feature Flag Management**: Dynamic feature enablement per role and configuration

## 🔌 Integration Points

### dbt Integration
- Model lineage ingestion
- Test result synchronization
- Documentation enrichment
- Metadata extraction

### External Tools
- **Collibra**: Catalog synchronization
- **JIRA**: Issue tracking integration
- **ServiceNow**: Incident management
- **Airflow**: Process orchestration

## 📋 Implementation Guide

### Phase 1: Foundation (Week 1-2)
1. **Deploy core infrastructure:**
   ```bash
   # Deploy database schemas and roles
   snowsql -f deployment/01_deployment_script.sql
   
   # Create application layer with security policies
   snowsql -f app_layer/unified_view.sql
   ```

2. **Bootstrap Streamlit application:**
   ```bash
   # Create app infrastructure
   snowsql -f deployment/app_build.sql
   
   # Upload application files
   snowsql -q "PUT file://streamlit_app/* @GOV_APP.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
   ```

3. **Load sample data and validate:**
   ```sql
   -- Load test data
   @sample_data/01_sample_data_generation.sql
   
   -- Validate deployment
   SELECT * FROM GOV_APP.VIEWS.VW_TODAY_HEALTH LIMIT 10;
   ```

### Phase 2: Security and Access Control (Week 2-3)
1. Configure role-based access policies
2. Set up row-level security and masking
3. Validate feature flag configurations
4. Test authorization workflows

### Phase 3: Data Integration (Week 3-4)
1. Integrate with dbt using provided macros
2. Configure data quality monitoring
3. Set up lineage capture
4. Implement automated data profiling

### Phase 4: Production Readiness (Week 4-5)
1. Performance tuning and optimization
2. Monitoring and alerting setup
3. Backup and recovery procedures
4. User training and documentation

## 🛠️ Configuration

### Application Infrastructure
The platform uses native Snowflake infrastructure:

```sql
-- Core databases
GOV_PLATFORM                -- Governance data and metadata
GOV_APP                     -- Application layer and configuration

-- Key warehouses
GOVERNANCE_APP_WH           -- Application compute warehouse
GOV_WH                      -- Alternative warehouse for app execution

-- Streamlit application
GOV_APP.APP.GOVERNANCE_APP  -- Main Streamlit application object
```

### Feature Flag Configuration
Control platform features through the `GOV_APP.CONFIG.APP_FEATURE_FLAG` table:

```sql
-- Available feature flags
TODAY_HEALTH_DASHBOARD      -- Real-time health metrics
DQ_RESULTS_EXPLORER        -- Data quality analysis tools
RISK_DASHBOARD             -- Risk management interface

-- Example feature flag update
UPDATE GOV_APP.CONFIG.APP_FEATURE_FLAG 
SET ENABLED = TRUE, 
    CONFIG_JSON = OBJECT_CONSTRUCT('refresh_interval_seconds', 300)
WHERE FEATURE_NAME = 'TODAY_HEALTH_DASHBOARD';
```

### Access Control Configuration
Manage page-level access through `GOV_APP.CONFIG.ROLE_PAGE_ACCESS`:

```sql
-- Configure role access to specific pages
INSERT INTO GOV_APP.CONFIG.ROLE_PAGE_ACCESS 
  (ROLE_NAME, PAGE_NAME, ACCESS_LEVEL)
VALUES 
  ('DATA_STEWARD', 'HOME', 'ALLOW'),
  ('GOVERNANCE_ANALYST', 'HOME', 'ALLOW');
```

## 📊 Monitoring & Maintenance

### Performance Monitoring
- Query performance tracking
- Resource utilization metrics
- User activity analytics
- System health dashboards

### Maintenance Tasks
- **Daily**: Data quality checks, process monitoring
- **Weekly**: Performance review, error analysis
- **Monthly**: Access review, compliance reporting
- **Quarterly**: Security audit, feature assessment

## 🚨 Troubleshooting

### Common Issues

1. **Access Denied Errors**
   - Check role assignments in `ROLE_PAGE_ACCESS`
   - Verify domain visibility in `DOMAIN_VISIBILITY`
   - Review masking policy applications

2. **Data Not Loading**
   - Verify secure view permissions
   - Check foreign key constraints
   - Review row-level security policies

3. **Performance Issues**
   - Optimize warehouse sizing
   - Review query patterns
   - Check clustering keys

## 📚 Documentation

- [Database Schema Reference](docs/database-schema.md)
- [API Documentation](docs/api-reference.md)
- [User Guide](docs/user-guide.md)
- [Administrator Guide](docs/admin-guide.md)
- [Integration Guide](docs/integration-guide.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with comprehensive tests
4. Submit a pull request with documentation

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: Create GitHub issues for bugs and feature requests
- **Documentation**: Check the docs/ directory
- **Community**: Join our Slack workspace
- **Enterprise**: Contact support@yourcompany.com

---

Built with ❤️ for enterprise data governance
