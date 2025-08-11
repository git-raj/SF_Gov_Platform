-- ====================================================================
-- OWNERSHIP SCHEMA - PEOPLE/TEAMS, STEWARDSHIP, RACI
-- ====================================================================

USE SCHEMA GOV_PLATFORM.OWNERSHIP;

-- Parties can be people or groups (team, distribution list)
CREATE OR REPLACE TABLE DIM_PARTY (
  PARTY_ID        STRING PRIMARY KEY COMMENT='Stable party key.',
  PARTY_TYPE      STRING NOT NULL COMMENT='PERSON or GROUP.',
  PARTY_NAME      STRING NOT NULL COMMENT='Display name (user full name or group name).',
  EMAIL           STRING COMMENT='Contact email for notifications/escalations.',
  MANAGER_PARTY_ID STRING COMMENT='Manager/group parent party id, if applicable.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='People and groups referenced by ownership and stewardship.';

-- Ownership mapping for datasets
CREATE OR REPLACE TABLE MAP_DATASET_OWNER (
  MAP_ID          STRING PRIMARY KEY COMMENT='Surrogate key.',
  DATASET_ID      STRING COMMENT='Target dataset.',
  PARTY_ID        STRING COMMENT='Owner/steward party.',
  ROLE_TYPE       STRING COMMENT='OWNER, STEWARD, CUSTODIAN, PRODUCER, CONSUMER.',
  EFFECTIVE_FROM  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT='Start timestamp of responsibility.',
  EFFECTIVE_TO    TIMESTAMP_TZ COMMENT='End timestamp if superseded.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='Dataset ownership/stewardship lifecycle with role types.';

-- RACI assignments for controls/policies
CREATE OR REPLACE TABLE MAP_RACI (
  MAP_ID          STRING PRIMARY KEY COMMENT='Surrogate key.',
  SCOPE_TYPE      STRING COMMENT='DATASET|POLICY|CONTROL|TERM|CDE.',
  SCOPE_ID        STRING COMMENT='Identifier of the governed object (e.g., DATASET_ID).',
  PARTY_ID        STRING COMMENT='Assigned party.',
  RACI_ROLE       STRING COMMENT='R|A|C|I.',
  CREATED_AT      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT      TIMESTAMP_TZ
) COMMENT='RACI matrix across governed scopes to clarify accountability.';

-- Add foreign key constraints
ALTER TABLE DIM_PARTY ADD CONSTRAINT FK_PARTY_MANAGER 
  FOREIGN KEY (MANAGER_PARTY_ID) REFERENCES DIM_PARTY(PARTY_ID);
ALTER TABLE MAP_DATASET_OWNER ADD CONSTRAINT FK_OWNER_DATASET 
  FOREIGN KEY (DATASET_ID) REFERENCES GOV_PLATFORM.CATALOG.DIM_DATASET(DATASET_ID);
ALTER TABLE MAP_DATASET_OWNER ADD CONSTRAINT FK_OWNER_PARTY 
  FOREIGN KEY (PARTY_ID) REFERENCES DIM_PARTY(PARTY_ID);
ALTER TABLE MAP_RACI ADD CONSTRAINT FK_RACI_PARTY 
  FOREIGN KEY (PARTY_ID) REFERENCES DIM_PARTY(PARTY_ID);
