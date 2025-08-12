-- =========================================================
-- Streamlit App Bootstrap (schemas, stage, app, grants)
-- Safe to run multiple times
-- =========================================================

-- ---------- Admin context ----------
USE ROLE ACCOUNTADMIN;

-- ---------- Databases & Schemas ----------
CREATE DATABASE IF NOT EXISTS GOV_APP COMMENT='Governance application layer';
CREATE SCHEMA    IF NOT EXISTS GOV_APP.VIEWS;
CREATE SCHEMA    IF NOT EXISTS GOV_APP.CONFIG;
CREATE SCHEMA    IF NOT EXISTS GOV_APP.POLICIES;

-- Optional: a dedicated schema for the stage (or keep in GOV_APP)
CREATE SCHEMA IF NOT EXISTS GOV_APP.APP;
USE DATABASE GOV_APP;

-- ---------- Internal Stage (for app code) ----------
-- Folder structure expected on stage:
--   @GOV_APP.APP_STAGE/app/Home.py
--   @GOV_APP.APP_STAGE/app/lib/*.py
CREATE STAGE IF NOT EXISTS APP.APP_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Streamlit app code (Home.py, lib/, pages/)';

-- ---------- (Optional) Warehouse for the app ----------
-- If you already have one, skip this and set QUERY_WAREHOUSE below.
CREATE WAREHOUSE IF NOT EXISTS GOV_WH
  WAREHOUSE_SIZE = XSMALL
  INITIALLY_SUSPENDED = TRUE
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for Governance Streamlit app';

-- ---------- Streamlit app object ----------
-- After you upload files to @GOV_APP.APP_STAGE/app, run/replace this.
CREATE OR REPLACE STREAMLIT GOVERNANCE_APP
  ROOT_LOCATION = '@GOV_APP.APP.APP_STAGE/app'  -- must contain Home.py
  MAIN_FILE     = 'Home.py'
  QUERY_WAREHOUSE = GOV_WH
  TITLE = 'Governance Platform';

-- -- Optional: include extra packages (Plotly export needs kaleido)
-- ALTER STREAMLIT GOV_APP.GOVERNANCE_APP
--   SET PACKAGES = ('pandas','plotly','snowflake-snowpark-python','kaleido');

-- SHOW STREAMLITS IN DATABASE GOV_APP;

-- ---------- Grants (adjust role names as needed) ----------
-- App viewers/operators
GRANT USAGE ON DATABASE GOV_APP TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON SCHEMA   GOV_APP.VIEWS TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON SCHEMA   GOV_APP.CONFIG TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON SCHEMA   GOV_APP.POLICIES TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON WAREHOUSE GOV_WH TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON STREAMLIT GOVERNANCE_APP TO ROLE GOVERNANCE_ADMIN;



-- If your app reads from GOV_PLATFORM, grant read access:
GRANT USAGE ON DATABASE GOV_PLATFORM TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE GOV_PLATFORM TO ROLE GOVERNANCE_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOV_PLATFORM.CATALOG    TO ROLE GOVERNANCE_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOV_PLATFORM.QUALITY    TO ROLE GOVERNANCE_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOV_PLATFORM.RISK       TO ROLE GOVERNANCE_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOV_PLATFORM.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOV_PLATFORM.LINEAGE    TO ROLE GOVERNANCE_ADMIN;

-- If you will attach row-access policies later, allow this capability:
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE GOVERNANCE_ADMIN;

-- ---------- Helpful: verify objects ----------
-- SHOW STAGES LIKE 'APP_STAGE' IN SCHEMA GOV_APP;
-- LIST @GOV_APP.APP_STAGE;
-- SHOW STREAMLITS IN DATABASE GOV_APP;
