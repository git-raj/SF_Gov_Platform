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
-- 1. Create secure views
@app_layer/01_secure_views.sql

-- 2. Apply security policies
@app_layer/02_security_policies.sql

-- 3. Set up configuration tables
@app_layer/03_config_tables.sql
```

### 3. Load Sample Data

```sql
-- Load sample data for testing
@sample_data/01_sample_data_generation.sql
```

### 4. Deploy Streamlit App

1. Create a new Streamlit app in Snowflake:
   ```sql
   CREATE STREAMLIT GOV_APP.STREAMLIT.GOVERNANCE_RECON_APP
   ROOT_LOCATION = '@internal_stage/streamlit_app/'
   MAIN_FILE = 'Home.py'
   QUERY_WAREHOUSE = 'GOVERNANCE_APP_WH';
   ```

2. Upload the Streamlit application files to the stage.

3. Grant appropriate permissions to user roles.

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
├── Home.py                 -- Main dashboard
├── pages/                  -- Application pages
│   ├── 1_🔍_DQ_Explorer.py
│   ├── 2_🧭_Lineage.py
│   ├── 3_👤_Ownership.py
│   └── 4_📜_Policies.py
└── lib/                    -- Utility libraries
    ├── dal.py              -- Data access layer
    ├── filters.py          -- Filter utilities
    ├── authz.py            -- Authorization
    └── charts.py           -- Visualization helpers
```

## 🔐 Security Model

### Role-Based Access Control

The platform implements a comprehensive RBAC model:

- **GOVERNANCE_ADMIN**: Full administrative access
- **DATA_STEWARD**: Data stewardship and quality management
- **GOVERNANCE_ANALYST**: Read-only analysis and reporting
- **AUDIT_ROLE**: Compliance and audit access
- **RISK_MANAGER**: Risk management focused access

### Data Protection

- **Row-Level Security**: Domain-based data segregation
- **Column Masking**: Automatic PII and sensitive data masking
- **Classification-Based Access**: Access control by data sensitivity
- **Audit Logging**: Comprehensive access and activity logging

## 📈 Key Features

### Today's Health Dashboard
- Real-time process execution status
- Data quality pass rates
- Control test results
- Exception monitoring

### Data Quality Explorer
- Rule registry and management
- Quality results analysis
- Evidence drill-down
- Trend analysis

### Lineage Visualization
- Upstream/downstream tracing
- Impact analysis
- Process dependencies
- Business lineage mapping

### Business Glossary
- Term definitions and stewardship
- Critical data element tracking
- Usage analytics
- Approval workflows

### Risk & Compliance
- Risk register management
- Control testing outcomes
- Policy compliance monitoring
- Attestation tracking

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
1. Deploy database schema
2. Configure basic security
3. Load sample data
4. Deploy Streamlit app

### Phase 2: Data Integration (Week 3-4)
1. Connect to source systems
2. Implement data ingestion
3. Configure lineage capture
4. Set up quality monitoring

### Phase 3: Process Integration (Week 5-6)
1. Integrate with dbt
2. Connect orchestration tools
3. Configure external APIs
4. Implement notifications

### Phase 4: Advanced Features (Week 7-8)
1. Advanced analytics
2. Custom dashboards
3. Automated workflows
4. Performance optimization

## 🛠️ Configuration

### Environment Variables
```yaml
# Snowflake Connection
SNOWFLAKE_ACCOUNT: your-account
SNOWFLAKE_USER: service-user
SNOWFLAKE_ROLE: GOVERNANCE_ADMIN
SNOWFLAKE_WAREHOUSE: GOVERNANCE_APP_WH

# External Integrations
COLLIBRA_BASE_URL: https://your-org.collibra.com
JIRA_BASE_URL: https://your-org.atlassian.net
SERVICENOW_URL: https://your-org.service-now.com
```

### Feature Flags
Control platform features through the `APP_FEATURE_FLAG` table:

```sql
-- Enable/disable features by role
UPDATE GOV_APP.CONFIG.APP_FEATURE_FLAG 
SET ENABLED = TRUE 
WHERE FEATURE_NAME = 'LINEAGE_VISUALIZATION';
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
