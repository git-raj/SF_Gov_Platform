-- ====================================================================
-- LINEAGE SCHEMA - GRAPH, PROCESSES, RUNS
-- ====================================================================

USE SCHEMA GOV_PLATFORM.LINEAGE;

-- Nodes represent datasets, jobs, reports, APIs, etc.
CREATE OR REPLACE TABLE LINEAGE_NODE (
  NODE_ID         STRING PRIMARY KEY COMMENT='Stable node id (dataset/job/report).',
  NODE_TYPE       STRING NOT NULL COMMENT='DATASET|JOB|REPORT|API.',
  REF_ID          STRING COMMENT='Foreign key reference to concrete object (e.g., DATASET_ID or PIPELINE_JOB_ID).',
  NAME            STRING COMMENT='Display name.',
  DESCRIPTION     STRING COMMENT='Description/notes.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Vertices in the lineage graph.';

-- Directed edges describe movement/derivation
CREATE OR REPLACE TABLE LINEAGE_EDGE (
  EDGE_ID         STRING PRIMARY KEY COMMENT='Surrogate key.',
  SRC_NODE_ID     STRING COMMENT='Upstream node id.',
  TGT_NODE_ID     STRING COMMENT='Downstream node id.',
  EDGE_TYPE       STRING COMMENT='LOADS|TRANSFORMS|DERIVES|CONSUMES.',
  LOGIC_REF       STRING COMMENT='Reference to code/SQL or dbt model name/sha.',
  ACTIVE_FLAG     BOOLEAN DEFAULT TRUE COMMENT='Active edge flag.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Edges (relationships) in the lineage graph.';

-- Processes and their runtime details (can link to Recon/DQ/ETL)
CREATE OR REPLACE TABLE PROCESS (
  PROCESS_ID      STRING PRIMARY KEY COMMENT='Process id (pipeline or job).',
  NAME            STRING NOT NULL COMMENT='Name of the process.',
  ORCHESTRATOR    STRING COMMENT='Airflow|Control-M|dbt|Qlik|Informatica|DataStage.',
  PIPELINE_JOB_ID STRING COMMENT='Link to CATALOG.DIM_PIPELINE_JOB if needed.',
  OWNER_PARTY_ID  STRING COMMENT='Operational owner.',
  DESCRIPTION     STRING COMMENT='Process summary.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Registered data processes (pipelines/jobs).';

CREATE OR REPLACE TABLE PROCESS_RUN (
  RUN_ID          STRING PRIMARY KEY COMMENT='Process run identifier (correlate with recon/dq).',
  PROCESS_ID      STRING COMMENT='Which process executed.',
  STARTED_AT      TIMESTAMP_TZ COMMENT='Run start.',
  ENDED_AT        TIMESTAMP_TZ COMMENT='Run end.',
  STATUS          STRING COMMENT='SUCCESS|FAILED|PARTIAL.',
  INPUT_SIGNATURE STRING COMMENT='Hash/signature of inputs.',
  OUTPUT_SIGNATURE STRING COMMENT='Hash/signature of outputs.',
  TRIGGER_REF     STRING COMMENT='Triggering event (schedule, file, API).',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Execution history for processes, used by lineage and governance reporting.';

-- Add foreign key constraints
ALTER TABLE LINEAGE_EDGE ADD CONSTRAINT FK_EDGE_SRC_NODE 
  FOREIGN KEY (SRC_NODE_ID) REFERENCES LINEAGE_NODE(NODE_ID);
ALTER TABLE LINEAGE_EDGE ADD CONSTRAINT FK_EDGE_TGT_NODE 
  FOREIGN KEY (TGT_NODE_ID) REFERENCES LINEAGE_NODE(NODE_ID);
ALTER TABLE PROCESS ADD CONSTRAINT FK_PROCESS_OWNER 
  FOREIGN KEY (OWNER_PARTY_ID) REFERENCES GOV_PLATFORM.OWNERSHIP.DIM_PARTY(PARTY_ID);
ALTER TABLE PROCESS_RUN ADD CONSTRAINT FK_RUN_PROCESS 
  FOREIGN KEY (PROCESS_ID) REFERENCES PROCESS(PROCESS_ID);
