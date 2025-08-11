-- ====================================================================
-- SNOWFLAKE GOVERNANCE PLATFORM - DATABASE & SCHEMA SETUP
-- ====================================================================
-- Enterprise governance database: catalog, lineage, controls, risk, security, quality, and change mgmt.

CREATE DATABASE IF NOT EXISTS GOV_PLATFORM COMMENT='Enterprise governance database: catalog, lineage, controls, risk, security, quality, and change mgmt.';

-- Core governance schemas
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.CATALOG  COMMENT='Authoritative data asset catalog, domains, terms, CDEs, and mappings.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.OWNERSHIP COMMENT='Stewardship, ownership, RACI and accountability mappings.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.GOVERNANCE COMMENT='Controls, policies, data contracts, classifications, retention rules.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.LINEAGE  COMMENT='Technical & business lineage graph, processes and runs.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.SECURITY COMMENT='Masking/row access policy registry, privacy assessments, access exceptions.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.RISK     COMMENT='Risk register, control tests, attestations.';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.QUALITY  COMMENT='Data quality rule registry and results (meta-level).';
CREATE SCHEMA IF NOT EXISTS GOV_PLATFORM.CHANGE   COMMENT='Change requests, approvals, certifications and releases.';

-- Application layer database
CREATE DATABASE IF NOT EXISTS GOV_APP COMMENT='Governance application layer with secure views and Streamlit app.';
CREATE SCHEMA IF NOT EXISTS GOV_APP.VIEWS COMMENT='Secure views for governance application access.';
CREATE SCHEMA IF NOT EXISTS GOV_APP.CONFIG COMMENT='Application configuration, feature flags, and RBAC settings.';
CREATE SCHEMA IF NOT EXISTS GOV_APP.POLICIES COMMENT='Data masking and row access policies.';

-- ====================================================================
-- CATALOG SCHEMA - DOMAINS, DATASETS, TERMS, CDEs
-- ====================================================================

USE SCHEMA GOV_PLATFORM.CATALOG;

-- Domains represent business areas (e.g., Deposits, Lending, Cards)
CREATE OR REPLACE TABLE DIM_DOMAIN (
  DOMAIN_ID       STRING PRIMARY KEY COMMENT='Stable surrogate key for business domain.',
  DOMAIN_NAME     STRING NOT NULL COMMENT='Human-friendly name of domain, unique.',
  DESCRIPTION     STRING COMMENT='Narrative description of domain scope.',
  CRITICALITY     STRING COMMENT='Business criticality (e.g., HIGH/MEDIUM/LOW).',
  OWNER_GROUP     STRING COMMENT='Primary owning group or product line.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT='Record creation time.',
  UPDATED_AT      TIMESTAMP_TZ COMMENT='Last update time.'
) COMMENT='Business domains for grouping datasets and governance scope.';

-- Systems are logical sources/targets (Core Banking, Mainframe, LOS)
CREATE OR REPLACE TABLE DIM_SYSTEM (
  SYSTEM_ID       STRING PRIMARY KEY COMMENT='Stable key for a logical system.',
  SYSTEM_NAME     STRING NOT NULL COMMENT='System name (unique).',
  SYSTEM_TYPE     STRING COMMENT='Type/category (Mainframe, SaaS, DB, Stream).',
  OWNER_GROUP     STRING COMMENT='Owning/operating technology group.',
  DESCRIPTION     STRING COMMENT='System description and boundaries.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Logical systems that produce or host datasets.';

-- Canonical dataset registry (ties to Snowflake objects and beyond)
CREATE OR REPLACE TABLE DIM_DATASET (
  DATASET_ID        STRING PRIMARY KEY COMMENT='Stable dataset identifier.',
  CATALOG_NAME      STRING COMMENT='External catalog name, if any (e.g., Collibra asset ID).',
  SYSTEM_ID         STRING COMMENT='Owning system.',
  DOMAIN_ID         STRING COMMENT='Owning domain.',
  PLATFORM          STRING COMMENT='Physical platform (Snowflake, S3, Kafka, Mainframe).',
  DATABASE_NAME     STRING COMMENT='Physical database/catalog name for Snowflake datasets.',
  SCHEMA_NAME       STRING COMMENT='Physical schema.',
  OBJECT_NAME       STRING COMMENT='Table/View/Stream name.',
  OBJECT_TYPE       STRING COMMENT='TABLE/VIEW/STREAM/EXTERNAL TABLE/etc.',
  DV_LAYER          STRING COMMENT='Medallion/Vault role (bronze/raw hub/link/sat/business_vault/gold).',
  CLASSIFICATION    STRING COMMENT='Sensitivity classification (e.g., PII, Confidential).',
  IS_CDE            BOOLEAN DEFAULT FALSE COMMENT='True if dataset contains one or more critical data elements.',
  DESCRIPTION       STRING COMMENT='Business description and usage.',
  RETENTION_POLICY_ID STRING COMMENT='Link to retention policy (GOVERNANCE.RETENTION_POLICY.POLICY_ID).',
  CERTIFICATION     STRING COMMENT='Certification status (e.g., Certified/Deprecated/Draft).',
  CREATED_AT        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT        TIMESTAMP_TZ
) COMMENT='Authoritative registry of datasets across the platform with business and technical context.';

-- Business glossary terms
CREATE OR REPLACE TABLE DIM_TERM (
  TERM_ID         STRING PRIMARY KEY COMMENT='Stable key for business term.',
  TERM_NAME       STRING NOT NULL COMMENT='Business term name (unique).',
  DEFINITION      STRING COMMENT='Authoritative definition of the term.',
  DOMAIN_ID       STRING COMMENT='Owning domain.',
  STEWARD_PARTY_ID STRING COMMENT='Primary steward party (OWNERSHIP.DIM_PARTY.PARTY_ID).',
  STATUS          STRING COMMENT='Draft/Approved/Deprecated.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Business glossary to standardize definitions.';

-- Critical Data Elements (CDEs) catalogue
CREATE OR REPLACE TABLE DIM_CDE (
  CDE_ID          STRING PRIMARY KEY COMMENT='Stable key for critical data element.',
  TERM_ID         STRING COMMENT='Associated glossary term.',
  NAME            STRING NOT NULL COMMENT='CDE name (often same as term name or attribute).',
  DESCRIPTION     STRING COMMENT='Context and materiality rationale.',
  MATERIALITY     STRING COMMENT='Materiality rating (e.g., HIGH/MEDIUM/LOW) for regulatory impact.',
  OWNER_GROUP     STRING COMMENT='Owning function (e.g., Finance Ops).',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Registry of critical data elements with materiality.';

-- Map dataset columns to terms/CDEs
CREATE OR REPLACE TABLE MAP_DATASET_ATTRIBUTE (
  MAP_ID          STRING PRIMARY KEY COMMENT='Surrogate key.',
  DATASET_ID      STRING COMMENT='Dataset containing the attribute.',
  COLUMN_NAME     STRING COMMENT='Physical column/field name.',
  TERM_ID         STRING COMMENT='Mapped business term.',
  CDE_ID          STRING COMMENT='Mapped CDE (optional).',
  SEMANTIC_TYPE   STRING COMMENT='Declared semantic type (IBAN, SSN, EMAIL, CURRENCY).',
  QUALITY_CRITICAL BOOLEAN DEFAULT FALSE COMMENT='True if attribute is in scope for DQ SLAs.',
  DESCRIPTION     STRING COMMENT='Additional notes about the mapping.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Column-level mapping of datasets to glossary terms and CDEs for governance coverage.';

-- Add foreign key constraints
ALTER TABLE DIM_DATASET ADD CONSTRAINT FK_DATASET_SYSTEM 
  FOREIGN KEY (SYSTEM_ID) REFERENCES DIM_SYSTEM(SYSTEM_ID);
ALTER TABLE DIM_DATASET ADD CONSTRAINT FK_DATASET_DOMAIN 
  FOREIGN KEY (DOMAIN_ID) REFERENCES DIM_DOMAIN(DOMAIN_ID);
ALTER TABLE DIM_TERM ADD CONSTRAINT FK_TERM_DOMAIN 
  FOREIGN KEY (DOMAIN_ID) REFERENCES DIM_DOMAIN(DOMAIN_ID);
ALTER TABLE DIM_CDE ADD CONSTRAINT FK_CDE_TERM 
  FOREIGN KEY (TERM_ID) REFERENCES DIM_TERM(TERM_ID);
ALTER TABLE MAP_DATASET_ATTRIBUTE ADD CONSTRAINT FK_ATTR_DATASET 
  FOREIGN KEY (DATASET_ID) REFERENCES DIM_DATASET(DATASET_ID);
ALTER TABLE MAP_DATASET_ATTRIBUTE ADD CONSTRAINT FK_ATTR_TERM 
  FOREIGN KEY (TERM_ID) REFERENCES DIM_TERM(TERM_ID);
ALTER TABLE MAP_DATASET_ATTRIBUTE ADD CONSTRAINT FK_ATTR_CDE 
  FOREIGN KEY (CDE_ID) REFERENCES DIM_CDE(CDE_ID);
