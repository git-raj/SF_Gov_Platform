-- ====================================================================
-- SAMPLE DATA GENERATION FOR GOVERNANCE PLATFORM
-- ====================================================================
-- Generate sample data for testing and demonstration purposes

-- Sample Domains
INSERT INTO GOV_PLATFORM.CATALOG.DIM_DOMAIN
(DOMAIN_ID, DOMAIN_NAME, DESCRIPTION, CRITICALITY, OWNER_GROUP)
VALUES
('DOM-001', 'Retail Banking', 'Customer deposits, checking, savings accounts', 'HIGH', 'Retail Operations'),
('DOM-002', 'Lending', 'Mortgages, personal loans, credit facilities', 'HIGH', 'Lending Operations'),
('DOM-003', 'Cards & Payments', 'Credit cards, debit cards, payment processing', 'HIGH', 'Cards Operations'),
('DOM-004', 'Treasury', 'Treasury management, liquidity, investments', 'HIGH', 'Treasury Operations'),
('DOM-005', 'Risk Management', 'Credit risk, market risk, operational risk', 'HIGH', 'Risk Management'),
('DOM-006', 'Compliance', 'Regulatory reporting, AML, KYC', 'HIGH', 'Compliance Team'),
('DOM-007', 'Human Resources', 'Employee data, payroll, benefits', 'MEDIUM', 'HR Operations'),
('DOM-008', 'Finance', 'Financial reporting, accounting, budgeting', 'HIGH', 'Finance Team');



-- Sample Systems
INSERT INTO GOV_PLATFORM.CATALOG.DIM_SYSTEM (SYSTEM_ID, SYSTEM_NAME, SYSTEM_TYPE, OWNER_GROUP, DESCRIPTION) VALUES
('SYS-001', 'Core Banking System', 'Mainframe', 'IT Operations', 'Legacy mainframe system for core banking'),
('SYS-002', 'Customer Data Platform', 'SaaS', 'Data Engineering', 'Salesforce-based customer management'),
('SYS-003', 'Lending Origination System', 'DB', 'Lending IT', 'Oracle-based loan origination'),
('SYS-004', 'Card Management System', 'SaaS', 'Cards IT', 'Third-party card processing platform'),
('SYS-005', 'Risk Analytics Platform', 'Stream', 'Risk IT', 'Real-time risk processing system'),
('SYS-006', 'Data Lake', 'DB', 'Data Engineering', 'AWS S3-based data lake'),
('SYS-007', 'Data Warehouse', 'DB', 'Data Engineering', 'Snowflake data warehouse'),
('SYS-008', 'Regulatory Reporting', 'SaaS', 'Compliance IT', 'Regulatory reporting platform');

-- Sample Datasets
INSERT INTO GOV_PLATFORM.CATALOG.DIM_DATASET (DATASET_ID, SYSTEM_ID, DOMAIN_ID, PLATFORM, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DV_LAYER, CLASSIFICATION, IS_CDE, DESCRIPTION, CERTIFICATION) VALUES
('DS-001', 'SYS-007', 'DOM-001', 'Snowflake', 'RETAIL_DW', 'CORE', 'CUSTOMER_ACCOUNT', 'TABLE', 'business_vault', 'PII', TRUE, 'Customer account master data', 'Certified'),
('DS-002', 'SYS-007', 'DOM-002', 'Snowflake', 'LENDING_DW', 'CORE', 'LOAN_APPLICATION', 'TABLE', 'business_vault', 'Confidential', TRUE, 'Loan application details', 'Certified'),
('DS-003', 'SYS-007', 'DOM-003', 'Snowflake', 'CARDS_DW', 'CORE', 'CARD_TRANSACTION', 'TABLE', 'business_vault', 'PCI', TRUE, 'Card transaction history', 'Certified'),
('DS-004', 'SYS-007', 'DOM-004', 'Snowflake', 'TREASURY_DW', 'CORE', 'LIQUIDITY_POSITION', 'TABLE', 'business_vault', 'Confidential', TRUE, 'Daily liquidity positions', 'Certified'),
('DS-005', 'SYS-007', 'DOM-005', 'Snowflake', 'RISK_DW', 'CORE', 'CREDIT_EXPOSURE', 'TABLE', 'business_vault', 'Confidential', TRUE, 'Credit risk exposures', 'Certified'),
('DS-006', 'SYS-006', 'DOM-001', 'S3', 'DATALAKE', 'RAW', 'CUSTOMER_EVENTS', 'EXTERNAL_TABLE', 'bronze', 'PII', FALSE, 'Raw customer event stream', 'Draft'),
('DS-007', 'SYS-006', 'DOM-002', 'S3', 'DATALAKE', 'RAW', 'LOAN_DOCUMENTS', 'EXTERNAL_TABLE', 'bronze', 'Confidential', FALSE, 'Loan application documents', 'UNDER_REVIEW'),
('DS-008', 'SYS-007', 'DOM-006', 'Snowflake', 'COMPLIANCE_DW', 'REPORTING', 'AML_ALERTS', 'TABLE', 'business_vault', 'Confidential', TRUE, 'Anti-money laundering alerts', 'Certified');

-- Sample Parties (Users and Groups)
INSERT INTO GOV_PLATFORM.OWNERSHIP.DIM_PARTY
  (PARTY_ID, PARTY_TYPE, PARTY_NAME, EMAIL)
VALUES
('PTY-001', 'PERSON', 'Alice Johnson', 'alice.johnson@company.com'),
('PTY-002', 'PERSON', 'Bob Smith', 'bob.smith@company.com'),
('PTY-003', 'PERSON', 'Carol Davis', 'carol.davis@company.com'),
('PTY-004', 'PERSON', 'David Wilson', 'david.wilson@company.com'),
('PTY-005', 'PERSON', 'Emma Brown', 'emma.brown@company.com'),
-- Groups/Teams
('PTY-100', 'GROUP', 'Data Governance Team', 'data-governance@company.com'),
('PTY-101', 'GROUP', 'Retail Data Team', 'retail-data@company.com'),
('PTY-102', 'GROUP', 'Lending Data Team', 'lending-data@company.com'),
('PTY-103', 'GROUP', 'Risk Analytics Team', 'risk-analytics@company.com'),
('PTY-104', 'GROUP', 'Compliance Analytics Team', 'compliance-analytics@company.com');

-- Sample Terms for Business Glossary
INSERT INTO GOV_PLATFORM.CATALOG.DIM_TERM (TERM_ID, TERM_NAME, DEFINITION, DOMAIN_ID, STEWARD_PARTY_ID, STATUS) VALUES
('TRM-001', 'Customer', 'An individual or entity that has a relationship with the bank', 'DOM-001', 'PTY-001', 'Approved'),
('TRM-002', 'Account Balance', 'The current amount of money in a customer account', 'DOM-001', 'PTY-001', 'Approved'),
('TRM-003', 'Credit Score', 'Numerical representation of creditworthiness', 'DOM-002', 'PTY-002', 'Approved'),
('TRM-004', 'Loan-to-Value Ratio', 'Ratio of loan amount to asset value', 'DOM-002', 'PTY-002', 'Approved'),
('TRM-005', 'Transaction Amount', 'Monetary value of a financial transaction', 'DOM-003', 'PTY-003', 'Approved'),
('TRM-006', 'Risk Exposure', 'Potential financial loss from risk factors', 'DOM-005', 'PTY-004', 'Approved'),
('TRM-007', 'Regulatory Capital', 'Capital required by regulatory authorities', 'DOM-004', 'PTY-005', 'Approved'),
('TRM-008', 'AML Alert', 'Automated alert for suspicious activity', 'DOM-006', 'PTY-001', 'Approved');

-- Sample Critical Data Elements (CDEs)
INSERT INTO GOV_PLATFORM.CATALOG.DIM_CDE (CDE_ID, TERM_ID, NAME, DESCRIPTION, MATERIALITY, OWNER_GROUP) VALUES
('CDE-001', 'TRM-002', 'Account Balance', 'Critical for financial reporting and capital calculations', 'HIGH', 'Finance Team'),
('CDE-002', 'TRM-003', 'Credit Score', 'Critical for risk management and lending decisions', 'HIGH', 'Risk Management'),
('CDE-003', 'TRM-004', 'Loan-to-Value Ratio', 'Critical for regulatory reporting and risk assessment', 'HIGH', 'Risk Management'),
('CDE-004', 'TRM-006', 'Risk Exposure', 'Critical for capital adequacy and stress testing', 'CRITICAL', 'Risk Management'),
('CDE-005', 'TRM-007', 'Regulatory Capital', 'Critical for regulatory compliance and reporting', 'CRITICAL', 'Finance Team');

-- Sample Dataset Ownership
INSERT INTO GOV_PLATFORM.OWNERSHIP.MAP_DATASET_OWNER (MAP_ID, DATASET_ID, PARTY_ID, ROLE_TYPE, EFFECTIVE_FROM) VALUES
('OWN-001', 'DS-001', 'PTY-001', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-002', 'DS-001', 'PTY-101', 'STEWARD', CURRENT_TIMESTAMP()),
('OWN-003', 'DS-002', 'PTY-002', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-004', 'DS-002', 'PTY-102', 'STEWARD', CURRENT_TIMESTAMP()),
('OWN-005', 'DS-003', 'PTY-003', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-006', 'DS-004', 'PTY-004', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-007', 'DS-005', 'PTY-004', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-008', 'DS-005', 'PTY-103', 'STEWARD', CURRENT_TIMESTAMP()),
('OWN-009', 'DS-008', 'PTY-005', 'OWNER', CURRENT_TIMESTAMP()),
('OWN-010', 'DS-008', 'PTY-104', 'STEWARD', CURRENT_TIMESTAMP());

-- Sample Classifications
INSERT INTO GOV_PLATFORM.GOVERNANCE.CLASSIFICATION (CLASS_ID, CLASS_NAME, DESCRIPTION) VALUES
('CLS-001', 'Public', 'Information that can be shared publicly'),
('CLS-002', 'Internal', 'Information for internal use only'),
('CLS-003', 'Confidential', 'Sensitive business information'),
('CLS-004', 'PII', 'Personally Identifiable Information'),
('CLS-005', 'PCI', 'Payment Card Industry sensitive data'),
('CLS-006', 'PHI', 'Protected Health Information');

-- Sample Policies
INSERT INTO GOV_PLATFORM.GOVERNANCE.POLICY_REGISTRY (POLICY_ID, POLICY_NAME, POLICY_TYPE, VERSION, EFFECTIVE_FROM, OWNER_PARTY_ID, TEXT) VALUES
('POL-001', 'Data Retention Policy', 'RETENTION', '1.0.0', '2024-01-01', 'PTY-100', 'Customer data retention for 7 years after account closure'),
('POL-002', 'PII Data Classification Policy', 'CLASSIFICATION', '1.0.0', '2024-01-01', 'PTY-100', 'All PII must be classified and protected according to privacy regulations'),
('POL-003', 'Data Quality Standards', 'QUALITY', '1.0.0', '2024-01-01', 'PTY-100', 'Data quality thresholds and measurement standards'),
('POL-004', 'Data Access Control Policy', 'ACCESS', '1.0.0', '2024-01-01', 'PTY-100', 'Role-based access control for sensitive data');

-- Sample Control Registry
INSERT INTO GOV_PLATFORM.GOVERNANCE.CONTROL_REGISTRY (CONTROL_ID, CONTROL_NAME, CONTROL_TYPE, POLICY_ID, SCOPE_TYPE, SCOPE_ID, DESCRIPTION, TOLERANCE_PCT, SEVERITY, OWNER_PARTY_ID, ENABLED_FLAG) VALUES
('CTL-001', 'Account Balance Completeness', 'DETECTIVE', 'POL-003', 'DATASET', 'DS-001', 'Ensure account balances are complete and not null', 5.0, 'HIGH', 'PTY-001', TRUE),
('CTL-002', 'Credit Score Validity', 'DETECTIVE', 'POL-003', 'DATASET', 'DS-002', 'Validate credit scores are within expected range', 2.0, 'CRITICAL', 'PTY-002', TRUE),
('CTL-003', 'Transaction Amount Accuracy', 'DETECTIVE', 'POL-003', 'DATASET', 'DS-003', 'Verify transaction amounts against source systems', 1.0, 'HIGH', 'PTY-003', TRUE),
('CTL-004', 'PII Data Classification', 'PREVENTIVE', 'POL-002', 'DATASET', 'DS-001', 'Ensure PII data is properly classified', 0.0, 'CRITICAL', 'PTY-100', TRUE),
('CTL-005', 'Risk Exposure Reconciliation', 'DETECTIVE', 'POL-003', 'DATASET', 'DS-005', 'Daily reconciliation of risk exposures', 3.0, 'CRITICAL', 'PTY-004', TRUE);

-- Sample Retention Policies
INSERT INTO GOV_PLATFORM.GOVERNANCE.RETENTION_POLICY (POLICY_ID, POLICY_NAME, DURATION_DAYS, DISPOSITION, LEGAL_HOLD_FLAG) VALUES
('RET-001', 'Customer Data - 7 Year Retention', 2555, 'ARCHIVE', TRUE),
('RET-002', 'Transaction Data - 10 Year Retention', 3650, 'ARCHIVE', TRUE),
('RET-003', 'Risk Data - 5 Year Retention', 1825, 'PURGE', FALSE),
('RET-004', 'Temporary Data - 90 Day Retention', 90, 'PURGE', FALSE);

-- Sample Data Quality Rules
INSERT INTO GOV_PLATFORM.QUALITY.DQ_RULE
  (RULE_ID, RULE_NAME, RULE_TYPE, DATASET_ID, COLUMN_NAME, CONFIG_JSON, SEVERITY, OWNER_PARTY_ID, ENABLED_FLAG)
SELECT 'DQR-001', 'Customer ID Not Null',      'COMPLETENESS', 'DS-001', 'CUSTOMER_ID',
       OBJECT_CONSTRUCT('threshold', 100.0, 'action', 'ALERT')::VARIANT, 'HIGH',   'PTY-001', TRUE
UNION ALL
SELECT 'DQR-002', 'Account Balance Positive',  'VALIDITY',     'DS-001', 'ACCOUNT_BALANCE',
       OBJECT_CONSTRUCT('min_value', 0, 'action', 'ALERT')::VARIANT,     'HIGH',   'PTY-001', TRUE
UNION ALL
SELECT 'DQR-003', 'Credit Score Range Check',  'VALIDITY',     'DS-002', 'CREDIT_SCORE',
       OBJECT_CONSTRUCT('min_value', 300, 'max_value', 850, 'action', 'ALERT')::VARIANT, 'HIGH', 'PTY-002', TRUE
UNION ALL
SELECT 'DQR-004', 'Transaction Amount Format', 'VALIDITY',     'DS-003', 'TRANSACTION_AMOUNT',
       OBJECT_CONSTRUCT('precision', 2, 'scale', 2, 'action', 'ALERT')::VARIANT,   'MEDIUM', 'PTY-003', TRUE
UNION ALL
SELECT 'DQR-005', 'Unique Customer Account',   'UNIQUENESS',   'DS-001', 'CUSTOMER_ID,ACCOUNT_NUMBER',
       OBJECT_CONSTRUCT('threshold', 100.0, 'action', 'ALERT')::VARIANT, 'HIGH',   'PTY-001', TRUE;



-- Sample Lineage Nodes
INSERT INTO GOV_PLATFORM.LINEAGE.LINEAGE_NODE (NODE_ID, NODE_TYPE, REF_ID, NAME, DESCRIPTION) VALUES
('LN-001', 'DATASET', 'DS-001', 'Customer Account Master', 'Core customer account data'),
('LN-002', 'DATASET', 'DS-002', 'Loan Applications', 'Loan application data'),
('LN-003', 'DATASET', 'DS-003', 'Card Transactions', 'Card transaction history'),
('LN-004', 'JOB', 'PROC-001', 'Daily Customer ETL', 'Daily customer data processing'),
('LN-005', 'JOB', 'PROC-002', 'Lending Data Pipeline', 'Lending data processing pipeline'),
('LN-006', 'REPORT', 'RPT-001', 'Customer Portfolio Report', 'Monthly customer portfolio analysis'),
('LN-007', 'DATASET', 'DS-006', 'Raw Customer Events', 'Raw customer activity stream');

-- Sample Lineage Edges
INSERT INTO GOV_PLATFORM.LINEAGE.LINEAGE_EDGE (EDGE_ID, SRC_NODE_ID, TGT_NODE_ID, EDGE_TYPE, LOGIC_REF, ACTIVE_FLAG) VALUES
('LE-001', 'LN-007', 'LN-001', 'TRANSFORMS', 'customer_etl.sql', TRUE),
('LE-002', 'LN-001', 'LN-006', 'CONSUMES', 'portfolio_report.py', TRUE),
('LE-003', 'LN-002', 'LN-006', 'CONSUMES', 'portfolio_report.py', TRUE),
('LE-004', 'LN-004', 'LN-001', 'LOADS', 'daily_customer_job', TRUE),
('LE-005', 'LN-005', 'LN-002', 'LOADS', 'lending_pipeline', TRUE);

-- Sample Processes
INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS (PROCESS_ID, NAME, ORCHESTRATOR, OWNER_PARTY_ID, DESCRIPTION) VALUES
('PROC-001', 'Daily Customer Data ETL', 'Airflow', 'PTY-101', 'Daily processing of customer data from source systems'),
('PROC-002', 'Lending Data Pipeline', 'dbt', 'PTY-102', 'Lending data transformation and quality checks'),
('PROC-003', 'Risk Analytics Pipeline', 'Airflow', 'PTY-103', 'Real-time risk calculation and aggregation'),
('PROC-004', 'Compliance Reporting Job', 'Control-M', 'PTY-104', 'Daily compliance and regulatory reporting'),
('PROC-005', 'Data Quality Monitoring', 'dbt', 'PTY-100', 'Automated data quality testing and alerting');

-- Sample Process Runs (Recent executions)
INSERT INTO GOV_PLATFORM.LINEAGE.PROCESS_RUN
  (RUN_ID, PROCESS_ID, STARTED_AT, ENDED_AT, STATUS, TRIGGER_REF)
VALUES
('RUN-001', 'PROC-001', DATEADD('hour', -2, CURRENT_TIMESTAMP()), DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'SUCCESS', 'SCHEDULED'),
('RUN-002', 'PROC-002', DATEADD('hour', -3, CURRENT_TIMESTAMP()), DATEADD('hour', -2, CURRENT_TIMESTAMP()), 'SUCCESS', 'SCHEDULED'),
('RUN-003', 'PROC-003', DATEADD('hour', -1, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'SUCCESS', 'SCHEDULED'),
('RUN-004', 'PROC-004', DATEADD('day', -1, CURRENT_TIMESTAMP()), DATEADD('minute', 30, DATEADD('day', -1, CURRENT_TIMESTAMP())), 'SUCCESS', 'SCHEDULED'),
('RUN-005', 'PROC-005', DATEADD('hour', -4, CURRENT_TIMESTAMP()), DATEADD('hour', -3, CURRENT_TIMESTAMP()), 'FAILED', 'SCHEDULED'),
('RUN-006', 'PROC-001', DATEADD('day', -1, CURRENT_TIMESTAMP()), DATEADD('minute', 45, DATEADD('day', -1, CURRENT_TIMESTAMP())), 'SUCCESS', 'SCHEDULED'),
('RUN-007', 'PROC-002', DATEADD('day', -1, CURRENT_TIMESTAMP()), DATEADD('minute', 25, DATEADD('day', -1, CURRENT_TIMESTAMP())), 'SUCCESS', 'SCHEDULED');


-- Sample Risk Items
INSERT INTO GOV_PLATFORM.RISK.RISK_ITEM
  (RISK_ID, TITLE, DESCRIPTION, CATEGORY, SEVERITY, LIKELIHOOD, IMPACT, OWNER_PARTY_ID, STATUS)
VALUES
('RSK-001', 'Data Quality Deterioration', 'Risk of declining data quality in critical datasets', 'DataQuality', 'HIGH',     'MEDIUM', 'HIGH', 'PTY-100', 'OPEN'),
('RSK-002', 'Regulatory Reporting Delays', 'Risk of missing regulatory reporting deadlines',      'Compliance',  'CRITICAL', 'LOW',    'HIGH', 'PTY-104', 'OPEN'),
('RSK-003', 'System Downtime Impact',      'Risk of core system outages affecting data processing','Operational','HIGH',     'MEDIUM', 'HIGH', 'PTY-101', 'MITIGATED'),
('RSK-004', 'Data Privacy Breach',         'Risk of unauthorized access to PII data',             'Security',    'CRITICAL', 'LOW',    'HIGH', 'PTY-100', 'OPEN');


-- Sample DQ Run and Results
INSERT INTO GOV_PLATFORM.QUALITY.DQ_RUN (RUN_ID, STARTED_AT, ENDED_AT, ORCHESTRATOR, STATUS) VALUES
('DQR-RUN-001', DATEADD('hour', -2, CURRENT_TIMESTAMP()), DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'dbt', 'SUCCESS'),
('DQR-RUN-002', DATEADD('day', -1, CURRENT_TIMESTAMP()), DATEADD('minute', 30, DATEADD('day', -1, CURRENT_TIMESTAMP())), 'dbt', 'SUCCESS'),
('DQR-RUN-003', DATEADD('hour', -4, CURRENT_TIMESTAMP()), DATEADD('hour', -3, CURRENT_TIMESTAMP()), 'dbt', 'FAILED');


INSERT INTO GOV_PLATFORM.QUALITY.DQ_RESULT
  (RUN_ID, RULE_ID, DATASET_ID, OUTCOME, METRICS_SUMMARY, EVIDENCE_REF)
SELECT 'DQR-RUN-001', 'DQR-001', 'DS-001', 'PASS',
       OBJECT_CONSTRUCT('completeness_rate', 99.8, 'null_count', 245)::VARIANT,
       'evidence/dqr-001/run-001.json'
UNION ALL
SELECT 'DQR-RUN-001', 'DQR-002', 'DS-001', 'PASS',
       OBJECT_CONSTRUCT('validity_rate', 98.5, 'negative_balance_count', 12)::VARIANT,
       'evidence/dqr-002/run-001.json'
UNION ALL
SELECT 'DQR-RUN-001', 'DQR-003', 'DS-002', 'FAIL',
       OBJECT_CONSTRUCT('validity_rate', 92.3, 'out_of_range_count', 156)::VARIANT,
       'evidence/dqr-003/run-001.json'
UNION ALL
SELECT 'DQR-RUN-002', 'DQR-001', 'DS-001', 'PASS',
       OBJECT_CONSTRUCT('completeness_rate', 99.9, 'null_count', 123)::VARIANT,
       'evidence/dqr-001/run-002.json'
UNION ALL
SELECT 'DQR-RUN-002', 'DQR-005', 'DS-001', 'WARN',
       OBJECT_CONSTRUCT('uniqueness_rate', 99.1, 'duplicate_count', 45)::VARIANT,
       'evidence/dqr-005/run-002.json';


-- Sample Control Test Results
INSERT INTO GOV_PLATFORM.RISK.CONTROL_TEST
  (TEST_ID, CONTROL_ID, RUN_ID, EXECUTED_AT, OUTCOME, DETAILS)
VALUES
('CT-001', 'CTL-001', 'RUN-001', DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'PASS', 'Account balance completeness: 99.8%'),
('CT-002', 'CTL-002', 'RUN-002', DATEADD('hour', -2, CURRENT_TIMESTAMP()), 'FAIL', 'Credit score validity: 92.3% - Below threshold'),
('CT-003', 'CTL-003', 'RUN-003', DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'PASS', 'Transaction amount accuracy: 99.9%'),
('CT-004', 'CTL-004', 'RUN-001', DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'PASS', 'PII classification: 100% compliant'),
('CT-005', 'CTL-005', 'RUN-003', DATEADD('hour', -1, CURRENT_TIMESTAMP()), 'WARN', 'Risk exposure reconciliation: 97.2%');


-- Sample Dataset Attribute Mappings
INSERT INTO GOV_PLATFORM.CATALOG.MAP_DATASET_ATTRIBUTE (MAP_ID, DATASET_ID, COLUMN_NAME, TERM_ID, CDE_ID, SEMANTIC_TYPE, QUALITY_CRITICAL) VALUES
('MAP-001', 'DS-001', 'CUSTOMER_ID', 'TRM-001', NULL, 'CUSTOMER_ID', TRUE),
('MAP-002', 'DS-001', 'ACCOUNT_BALANCE', 'TRM-002', 'CDE-001', 'CURRENCY', TRUE),
('MAP-003', 'DS-002', 'CREDIT_SCORE', 'TRM-003', 'CDE-002', 'SCORE', TRUE),
('MAP-004', 'DS-002', 'LOAN_TO_VALUE_RATIO', 'TRM-004', 'CDE-003', 'PERCENTAGE', TRUE),
('MAP-005', 'DS-003', 'TRANSACTION_AMOUNT', 'TRM-005', NULL, 'CURRENCY', TRUE),
('MAP-006', 'DS-005', 'RISK_EXPOSURE_AMOUNT', 'TRM-006', 'CDE-004', 'CURRENCY', TRUE),
('MAP-007', 'DS-004', 'REGULATORY_CAPITAL', 'TRM-007', 'CDE-005', 'CURRENCY', TRUE);

-- Update dataset with retention policies
UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET 
SET RETENTION_POLICY_ID = 'RET-001'
WHERE DATASET_ID IN ('DS-001', 'DS-006');

UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET 
SET RETENTION_POLICY_ID = 'RET-002'
WHERE DATASET_ID = 'DS-003';

UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET 
SET RETENTION_POLICY_ID = 'RET-003'
WHERE DATASET_ID IN ('DS-004', 'DS-005');

-- Link Foreign Keys for Dataset
UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET SET SYSTEM_ID = 'SYS-007' WHERE DATASET_ID IN ('DS-001', 'DS-002', 'DS-003', 'DS-004', 'DS-005', 'DS-008');
UPDATE GOV_PLATFORM.CATALOG.DIM_DATASET SET SYSTEM_ID = 'SYS-006' WHERE DATASET_ID IN ('DS-006', 'DS-007');

COMMIT;