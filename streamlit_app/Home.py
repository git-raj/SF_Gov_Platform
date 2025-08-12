import streamlit as st
import pandas as pd
from lib import dal, filters, authz, charts

st.set_page_config(page_title="Governance Platform", page_icon="✅", layout="wide")

# --- Session & DAL ----------------------------------------------------
db = dal.DAL()  # uses get_active_session() inside

# --- Header -----------------------------------------------------------
st.title("Governance Platform – Home")

# Simulated role selector (Snowflake role used when available)
with st.sidebar:
    st.header("Context")
    role = db.get_current_role() or st.session_state.get("role") or "GOVERNANCE_ADMIN"
    role = st.selectbox("Role", [role, "GOVERNANCE_ADMIN", "DATA_STEWARD", "ANALYST"])

# --- AuthZ: default-allow on first run --------------------------------
page_name = "HOME"
allowed, reason = authz.page_visible(db, role, page_name)
db.log_access_attempt(role, page_name, allowed, reason)
if not allowed:
    st.error("You are not allowed to view this page.")
    st.stop()

# --- Filters -----------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    domains = filters.get_domains(db)
    systems = filters.get_systems(db)
    domain = st.selectbox("Domain", ["(All)"] + domains) if domains else "(All)"
    system = st.selectbox("System", ["(All)"] + systems) if systems else "(All)"

# --- KPI Row -----------------------------------------------------------
kpi_cols = st.columns(4)
try:
    datasets_cnt = db.scalar("SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_DATASET")
except Exception:
    datasets_cnt = 0

try:
    terms_cnt = db.scalar("SELECT COUNT(*) FROM GOV_PLATFORM.CATALOG.DIM_TERM")
except Exception:
    terms_cnt = 0

try:
    dq_rules_cnt = db.scalar("SELECT COUNT(*) FROM GOV_PLATFORM.QUALITY.DQ_RULE")
except Exception:
    dq_rules_cnt = 0

try:
    risks_open_cnt = db.scalar("SELECT COUNT(*) FROM GOV_PLATFORM.RISK.RISK_ITEM WHERE STATUS='OPEN'")
except Exception:
    risks_open_cnt = 0

charts.kpi_card(kpi_cols[0], "Datasets", datasets_cnt)
charts.kpi_card(kpi_cols[1], "Glossary Terms", terms_cnt)
charts.kpi_card(kpi_cols[2], "DQ Rules", dq_rules_cnt)
charts.kpi_card(kpi_cols[3], "Open Risks", risks_open_cnt)

# --- Dataset table (respecting filters) --------------------------------
st.subheader("Datasets")
df_datasets = db.query(
    '''
    SELECT DATASET_ID, SYSTEM_ID, DOMAIN_ID, PLATFORM, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DV_LAYER, CLASSIFICATION, CERTIFICATION
    FROM GOV_PLATFORM.CATALOG.DIM_DATASET
    WHERE (UPPER(:domain) = '(ALL)' OR DOMAIN_ID = :domain)
      AND (UPPER(:system) = '(ALL)' OR SYSTEM_ID = :system)
    ORDER BY DOMAIN_ID, SYSTEM_ID, OBJECT_NAME
    ''',
    {"domain": domain, "system": system}
)
st.dataframe(df_datasets, use_container_width=True)

# --- DQ Outcomes sample chart -----------------------------------------
st.subheader("Recent Data Quality Results")
df_dq = db.query(
    '''
    SELECT R.RUN_ID, RES.RULE_ID, RES.DATASET_ID, RES.OUTCOME,
           TRY_TO_DECIMAL(RES.METRICS_SUMMARY:"validity_rate") AS validity_rate,
           TRY_TO_DECIMAL(RES.METRICS_SUMMARY:"completeness_rate") AS completeness_rate
    FROM GOV_PLATFORM.QUALITY.DQ_RESULT RES
    JOIN GOV_PLATFORM.QUALITY.DQ_RUN R ON R.RUN_ID = RES.RUN_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY RES.RULE_ID ORDER BY R.STARTED_AT DESC) = 1
    ORDER BY RES.RULE_ID
    '''
)

chart = charts.outcome_chart(df_dq)
st.altair_chart(chart, use_container_width=True)

# --- Risk items table --------------------------------------------------
st.subheader("Risk Register (Open/Mitigated)")
df_risk = db.query(
    '''
    SELECT RISK_ID, TITLE, CATEGORY, SEVERITY, LIKELIHOOD, IMPACT, OWNER_PARTY_ID, STATUS
    FROM GOV_PLATFORM.RISK.RISK_ITEM
    WHERE STATUS IN ('OPEN','MITIGATED')
    ORDER BY SEVERITY DESC, IMPACT DESC
    '''
)
st.dataframe(df_risk, use_container_width=True)


