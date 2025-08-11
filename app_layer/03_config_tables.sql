-- ====================================================================
-- GOVERNANCE APPLICATION LAYER - CONFIGURATION TABLES
-- ====================================================================
-- Configuration tables for feature flags, RBAC, and app settings

USE SCHEMA GOV_APP.CONFIG;

-- Application feature flags
CREATE OR REPLACE TABLE APP_FEATURE_FLAG (
  FEATURE_NAME    STRING PRIMARY KEY COMMENT='Name of the feature flag.',
  ENABLED         BOOLEAN DEFAULT TRUE COMMENT='Whether the feature is enabled.',
  DESCRIPTION     STRING COMMENT='Description of what the feature does.',
  ROLES_ALLOWED   ARRAY COMMENT='Array of Snowflake roles allowed to use this feature.',
  CONFIG_JSON     VARIANT COMMENT='Additional configuration parameters for the feature.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Feature flags to control application functionality by role.';

-- Role-based page access control
CREATE OR REPLACE TABLE ROLE_PAGE_ACCESS (
  ROLE_NAME       STRING COMMENT='Snowflake role name.',
  PAGE_NAME       STRING COMMENT='Application page identifier.',
  ACCESS_LEVEL    STRING COMMENT='READ|WRITE|ADMIN.',
  DESCRIPTION     STRING COMMENT='Description of access granted.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ,
  PRIMARY KEY (ROLE_NAME, PAGE_NAME)
) COMMENT='Role-based access control for application pages.';

-- Domain visibility control
CREATE OR REPLACE TABLE DOMAIN_VISIBILITY (
  ROLE_NAME       STRING COMMENT='Snowflake role name.',
  DOMAIN_NAME     STRING COMMENT='Business domain name.',
  ACCESS_TYPE     STRING COMMENT='FULL|LIMITED|NONE.',
  RESTRICTIONS    VARIANT COMMENT='JSON object describing any access restrictions.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ,
  PRIMARY KEY (ROLE_NAME, DOMAIN_NAME)
) COMMENT='Domain-level access control for data segregation.';

-- Application telemetry and usage tracking
CREATE OR REPLACE TABLE APP_TELEMETRY (
  SESSION_ID      STRING COMMENT='Unique session identifier.',
  USER_NAME       STRING COMMENT='Snowflake username.',
  ROLE_NAME       STRING COMMENT='Active role during session.',
  PAGE_NAME       STRING COMMENT='Page accessed.',
  ACTION          STRING COMMENT='Action performed (VIEW|FILTER|EXPORT|etc.).',
  DURATION_MS     NUMBER(10,0) COMMENT='Time spent on action in milliseconds.',
  QUERY_COUNT     NUMBER(10,0) COMMENT='Number of queries executed.',
  ERROR_MESSAGE   STRING COMMENT='Error message if action failed.',
  TIMESTAMP_TZ    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT='When the action occurred.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT='Application usage telemetry for monitoring and optimization.';

-- Notification preferences
CREATE OR REPLACE TABLE NOTIFICATION_PREFERENCES (
  USER_NAME       STRING COMMENT='Snowflake username.',
  NOTIFICATION_TYPE STRING COMMENT='Type of notification (ALERT|SUMMARY|APPROVAL|etc.).',
  DELIVERY_METHOD STRING COMMENT='EMAIL|SLACK|IN_APP.',
  ENABLED         BOOLEAN DEFAULT TRUE COMMENT='Whether notifications are enabled.',
  FREQUENCY       STRING COMMENT='IMMEDIATE|DAILY|WEEKLY.',
  FILTER_CRITERIA VARIANT COMMENT='JSON criteria for filtering notifications.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ,
  PRIMARY KEY (USER_NAME, NOTIFICATION_TYPE)
) COMMENT='User notification preferences and delivery settings.';

-- External system integration settings
CREATE OR REPLACE TABLE INTEGRATION_CONFIG (
  SYSTEM_NAME     STRING PRIMARY KEY COMMENT='Name of external system (JIRA|COLLIBRA|SERVICENOW).',
  ENABLED         BOOLEAN DEFAULT TRUE COMMENT='Whether integration is enabled.',
  BASE_URL        STRING COMMENT='Base URL for the external system.',
  AUTH_METHOD     STRING COMMENT='Authentication method (OAUTH|API_KEY|BASIC).',
  CONFIG_JSON     VARIANT COMMENT='System-specific configuration parameters.',
  LAST_SYNC_AT    TIMESTAMP_TZ COMMENT='Last successful sync timestamp.',
  SYNC_STATUS     STRING COMMENT='SUCCESS|FAILED|IN_PROGRESS.',
  ERROR_MESSAGE   STRING COMMENT='Last sync error message if any.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Configuration for external system integrations.';

-- ====================================================================
-- INSERT DEFAULT CONFIGURATION DATA
-- ====================================================================

-- Default feature flags
INSERT INTO APP_FEATURE_FLAG (FEATURE_NAME, ENABLED, DESCRIPTION, ROLES_ALLOWED, CONFIG_JSON) VALUES
('TODAY_HEALTH_DASHBOARD', TRUE, 'Today''s health dashboard with real-time metrics', 
 ['GOVERNANCE_ADMIN', 'DATA_STEWARD', 'GOVERNANCE_ANALYST'], 
 {'refresh_interval_seconds': 300, 'max_rows_display': 1000}),
 
('DQ_RESULTS_EXPLORER', TRUE, 'Data quality results exploration and drill-down', 
 ['GOVERNANCE_ADMIN', 'DATA_STEWARD', 'GOVERNANCE_ANALYST'], 
 {'enable_evidence_links': true, 'max_export_rows': 10000}),
 
('LINEAGE_VISUALIZATION', TRUE, 'Interactive data lineage visualization', 
 ['GOVERNANCE_ADMIN', 'DATA_STEWARD', 'GOVERNANCE_ANALYST'], 
 {'max_depth_levels': 5, 'enable_impact_analysis': true}),
 
('CONTROL_TESTING_RESULTS', TRUE, 'Control testing outcomes and trends', 
 ['GOVERNANCE_ADMIN', 'AUDIT_ROLE', 'RISK_MANAGER'], 
 {'show_tolerance_analysis': true, 'enable_attestations': true}),
 
('BUSINESS_GLOSSARY', TRUE, 'Business glossary search and management', 
 ['GOVERNANCE_ADMIN', 'DATA_STEWARD', 'GOVERNANCE_ANALYST'], 
 {'enable_term_suggestions': true, 'auto_mapping': false}),
 
('DATA_CONTRACTS', TRUE, 'Data contracts monitoring and SLA tracking', 
 ['GOVERNANCE_ADMIN', 'DATA_STEWARD', 'PRODUCT_OWNER'], 
 {'sla_alert_threshold_hours': 2, 'enable_contract_validation': true}),
 
('RISK_DASHBOARD', TRUE, 'Enterprise risk dashboard and reporting', 
 ['GOVERNANCE_ADMIN', 'RISK_MANAGER', 'AUDIT_ROLE'], 
 {'risk_scoring_model': 'v2', 'enable_predictive_alerts': true}),
 
('ADMIN_PANEL', TRUE, 'Administrative functions and system management', 
 ['GOVERNANCE_ADMIN'], 
 {'enable_bulk_operations': true, 'show_system_metrics': true});

-- Default role-based page access
INSERT INTO ROLE_PAGE_ACCESS (ROLE_NAME, PAGE_NAME, ACCESS_LEVEL, DESCRIPTION) VALUES
-- Governance Admin - Full access to all pages
('GOVERNANCE_ADMIN', 'HOME', 'ADMIN', 'Full administrative access to dashboard'),
('GOVERNANCE_ADMIN', 'DQ_EXPLORER', 'ADMIN', 'Full access to data quality explorer'),
('GOVERNANCE_ADMIN', 'LINEAGE', 'ADMIN', 'Full access to lineage visualization'),
('GOVERNANCE_ADMIN', 'OWNERSHIP', 'ADMIN', 'Full access to ownership management'),
('GOVERNANCE_ADMIN', 'POLICIES', 'ADMIN', 'Full access to policies and contracts'),
('GOVERNANCE_ADMIN', 'ADMIN', 'ADMIN', 'Administrative panel access'),

-- Data Steward - Read/Write access to most pages
('DATA_STEWARD', 'HOME', 'WRITE', 'Dashboard access with update capabilities'),
('DATA_STEWARD', 'DQ_EXPLORER', 'WRITE', 'DQ explorer with result annotation'),
('DATA_STEWARD', 'LINEAGE', 'READ', 'Read-only lineage visualization'),
('DATA_STEWARD', 'OWNERSHIP', 'WRITE', 'Ownership information management'),
('DATA_STEWARD', 'POLICIES', 'READ', 'Read-only policy and contract viewing'),

-- Governance Analyst - Read access with limited write
('GOVERNANCE_ANALYST', 'HOME', 'READ', 'Dashboard viewing'),
('GOVERNANCE_ANALYST', 'DQ_EXPLORER', 'READ', 'DQ results viewing'),
('GOVERNANCE_ANALYST', 'LINEAGE', 'READ', 'Lineage visualization'),
('GOVERNANCE_ANALYST', 'OWNERSHIP', 'READ', 'Ownership information viewing'),
('GOVERNANCE_ANALYST', 'POLICIES', 'READ', 'Policy viewing'),

-- Audit Role - Read access to compliance-relevant pages
('AUDIT_ROLE', 'HOME', 'READ', 'Dashboard for audit purposes'),
('AUDIT_ROLE', 'DQ_EXPLORER', 'READ', 'DQ results for audit trails'),
('AUDIT_ROLE', 'OWNERSHIP', 'READ', 'Ownership verification'),
('AUDIT_ROLE', 'POLICIES', 'READ', 'Policy compliance verification'),

-- Risk Manager - Risk-focused access
('RISK_MANAGER', 'HOME', 'READ', 'Risk-focused dashboard view'),
('RISK_MANAGER', 'DQ_EXPLORER', 'READ', 'DQ results for risk assessment'),
('RISK_MANAGER', 'POLICIES', 'READ', 'Policy compliance monitoring');

-- Default domain visibility (example domains)
INSERT INTO DOMAIN_VISIBILITY (ROLE_NAME, DOMAIN_NAME, ACCESS_TYPE, RESTRICTIONS) VALUES
('GOVERNANCE_ADMIN', 'Retail', 'FULL', NULL),
('GOVERNANCE_ADMIN', 'Lending', 'FULL', NULL),
('GOVERNANCE_ADMIN', 'Cards', 'FULL', NULL),
('GOVERNANCE_ADMIN', 'Deposits', 'FULL', NULL),

('RETAIL_DATA_ANALYST', 'Retail', 'FULL', NULL),
('RETAIL_DATA_ANALYST', 'Lending', 'NONE', NULL),
('RETAIL_DATA_ANALYST', 'Cards', 'NONE', NULL),
('RETAIL_DATA_ANALYST', 'Deposits', 'NONE', NULL),

('LENDING_DATA_ANALYST', 'Retail', 'NONE', NULL),
('LENDING_DATA_ANALYST', 'Lending', 'FULL', NULL),
('LENDING_DATA_ANALYST', 'Cards', 'LIMITED', {'access_level': 'metadata_only'}),
('LENDING_DATA_ANALYST', 'Deposits', 'LIMITED', {'access_level': 'metadata_only'}),

('DATA_STEWARD', 'Retail', 'FULL', NULL),
('DATA_STEWARD', 'Lending', 'FULL', NULL),
('DATA_STEWARD', 'Cards', 'FULL', NULL),
('DATA_STEWARD', 'Deposits', 'FULL', NULL);

-- Default integration configurations (placeholder entries)
INSERT INTO INTEGRATION_CONFIG (SYSTEM_NAME, ENABLED, BASE_URL, AUTH_METHOD, CONFIG_JSON) VALUES
('COLLIBRA', FALSE, 'https://your-org.collibra.com/rest', 'OAUTH', 
 {'client_id': 'placeholder', 'scopes': ['READ', 'WRITE'], 'sync_frequency': 'daily'}),
 
('JIRA', FALSE, 'https://your-org.atlassian.net/rest/api/3', 'API_KEY', 
 {'project_key': 'GOV', 'issue_type': 'Task', 'auto_create_tickets': false}),
 
('SERVICENOW', FALSE, 'https://your-org.service-now.com/api', 'BASIC', 
 {'table_name': 'incident', 'auto_escalate': false, 'priority_mapping': {'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}});
