"""
Data Access Layer (DAL) for Snowflake Governance Platform
Provides centralized data access functions using Snowpark
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import DataFrame
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import uuid

class GovernanceDAL:
    """Data Access Layer for Governance Platform"""
    
    def __init__(self):
        self.session = get_active_session()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute SQL query and return pandas DataFrame"""
        try:
            if params:
                result = self.session.sql(sql, params=params)
            else:
                result = self.session.sql(sql)
            return result.to_pandas()
        except Exception as e:
            print(f"Query execution error: {e}")
            return pd.DataFrame()
    
    def today_health(self, domain: Optional[str] = None, process: Optional[str] = None) -> pd.DataFrame:
        """Get today's health metrics from secure view"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_TODAY_HEALTH WHERE 1=1"
        params = []
        
        if domain:
            sql += " AND DOMAIN_NAME = ?"
            params.append(domain)
        
        if process:
            sql += " AND PROCESS_NAME = ?"
            params.append(process)
        
        sql += " ORDER BY STARTED_AT DESC"
        
        return self.execute_query(sql, params if params else None)
    
    def dq_results(self, limit: int = 500, domain: Optional[str] = None, 
                   severity: Optional[str] = None, outcome: Optional[str] = None) -> pd.DataFrame:
        """Get data quality results with enriched context"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_DQ_RESULTS_ENRICHED WHERE 1=1"
        params = []
        
        if domain:
            sql += " AND DOMAIN_NAME = ?"
            params.append(domain)
        
        if severity:
            sql += " AND SEVERITY = ?"
            params.append(severity)
        
        if outcome:
            sql += " AND OUTCOME = ?"
            params.append(outcome)
        
        sql += f" ORDER BY CREATED_AT DESC LIMIT {limit}"
        
        return self.execute_query(sql, params if params else None)
    
    def control_results(self, limit: int = 500, control_type: Optional[str] = None,
                       outcome: Optional[str] = None) -> pd.DataFrame:
        """Get control test results with governance context"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_CONTROL_RESULTS_ENRICHED WHERE 1=1"
        params = []
        
        if control_type:
            sql += " AND CONTROL_TYPE = ?"
            params.append(control_type)
        
        if outcome:
            sql += " AND OUTCOME = ?"
            params.append(outcome)
        
        sql += f" ORDER BY EXECUTED_AT DESC LIMIT {limit}"
        
        return self.execute_query(sql, params if params else None)
    
    def dataset_owners(self, domain: Optional[str] = None) -> pd.DataFrame:
        """Get dataset ownership information"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_DATASET_OWNERS WHERE 1=1"
        params = []
        
        if domain:
            sql += " AND DOMAIN_NAME = ?"
            params.append(domain)
        
        sql += " ORDER BY DOMAIN_NAME, OBJECT_NAME"
        
        return self.execute_query(sql, params if params else None)
    
    def lineage_edges(self, node_name: Optional[str] = None, active_only: bool = True) -> pd.DataFrame:
        """Get lineage edges for visualization"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_LINEAGE_EDGES WHERE 1=1"
        params = []
        
        if active_only:
            sql += " AND ACTIVE_FLAG = TRUE"
        
        if node_name:
            sql += " AND (SRC_FULL_NAME ILIKE ? OR TGT_FULL_NAME ILIKE ?)"
            params.extend([f"%{node_name}%", f"%{node_name}%"])
        
        return self.execute_query(sql, params if params else None)
    
    def business_glossary(self, search_term: Optional[str] = None, 
                         domain: Optional[str] = None) -> pd.DataFrame:
        """Get business glossary with usage statistics"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_BUSINESS_GLOSSARY WHERE 1=1"
        params = []
        
        if search_term:
            sql += " AND (TERM_NAME ILIKE ? OR DEFINITION ILIKE ?)"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        
        if domain:
            sql += " AND DOMAIN_NAME = ?"
            params.append(domain)
        
        sql += " ORDER BY TERM_NAME"
        
        return self.execute_query(sql, params if params else None)
    
    def data_contracts(self, status: Optional[str] = None, 
                      domain: Optional[str] = None) -> pd.DataFrame:
        """Get data contracts with SLA monitoring"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_DATA_CONTRACTS WHERE 1=1"
        params = []
        
        if status:
            sql += " AND STATUS = ?"
            params.append(status)
        
        if domain:
            sql += " AND DOMAIN_NAME = ?"
            params.append(domain)
        
        sql += " ORDER BY EFFECTIVE_FROM DESC"
        
        return self.execute_query(sql, params if params else None)
    
    def risk_dashboard(self, category: Optional[str] = None,
                      severity: Optional[str] = None) -> pd.DataFrame:
        """Get risk dashboard data"""
        sql = "SELECT * FROM GOV_APP.VIEWS.VW_RISK_DASHBOARD WHERE 1=1"
        params = []
        
        if category:
            sql += " AND CATEGORY = ?"
            params.append(category)
        
        if severity:
            sql += " AND SEVERITY = ?"
            params.append(severity)
        
        sql += " ORDER BY SEVERITY DESC, LIKELIHOOD DESC"
        
        return self.execute_query(sql, params if params else None)
    
    def get_domains(self) -> List[str]:
        """Get list of available domains"""
        sql = "SELECT DISTINCT DOMAIN_NAME FROM GOV_PLATFORM.CATALOG.DIM_DOMAIN ORDER BY DOMAIN_NAME"
        df = self.execute_query(sql)
        return df['DOMAIN_NAME'].tolist() if not df.empty else []
    
    def get_processes(self) -> List[str]:
        """Get list of available processes"""
        sql = "SELECT DISTINCT NAME FROM GOV_PLATFORM.LINEAGE.PROCESS ORDER BY NAME"
        df = self.execute_query(sql)
        return df['NAME'].tolist() if not df.empty else []
    
    def get_feature_flags(self, role_name: str) -> Dict[str, Any]:
        """Get feature flags for the current role"""
        sql = """
        SELECT FEATURE_NAME, ENABLED, CONFIG_JSON
        FROM GOV_APP.CONFIG.APP_FEATURE_FLAG 
        WHERE ARRAY_CONTAINS(?, ROLES_ALLOWED) OR ROLES_ALLOWED IS NULL
        """
        df = self.execute_query(sql, [role_name])
        
        flags = {}
        for _, row in df.iterrows():
            flags[row['FEATURE_NAME']] = {
                'enabled': row['ENABLED'],
                'config': json.loads(row['CONFIG_JSON']) if row['CONFIG_JSON'] else {}
            }
        return flags
    
    def check_page_access(self, role_name: str, page_name: str) -> Optional[str]:
        """Check page access level for role"""
        sql = """
        SELECT ACCESS_LEVEL 
        FROM GOV_APP.CONFIG.ROLE_PAGE_ACCESS 
        WHERE ROLE_NAME = ? AND PAGE_NAME = ?
        """
        df = self.execute_query(sql, [role_name, page_name])
        return df['ACCESS_LEVEL'].iloc[0] if not df.empty else None
    
    def check_domain_access(self, role_name: str, domain_name: str) -> Optional[str]:
        """Check domain access type for role"""
        sql = """
        SELECT ACCESS_TYPE, RESTRICTIONS
        FROM GOV_APP.CONFIG.DOMAIN_VISIBILITY 
        WHERE ROLE_NAME = ? AND DOMAIN_NAME = ?
        """
        df = self.execute_query(sql, [role_name, domain_name])
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    
    def log_telemetry(self, user_name: str, role_name: str, page_name: str, 
                     action: str, duration_ms: int = 0, query_count: int = 0,
                     filters: Optional[Dict] = None, error_message: Optional[str] = None):
        """Log application telemetry"""
        try:
            session_id = str(uuid.uuid4())
            
            sql = """
            INSERT INTO GOV_APP.CONFIG.APP_TELEMETRY 
            (SESSION_ID, USER_NAME, ROLE_NAME, PAGE_NAME, ACTION, DURATION_MS, QUERY_COUNT, ERROR_MESSAGE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.session.sql(sql, params=[
                session_id, user_name, role_name, page_name, action, 
                duration_ms, query_count, error_message
            ]).collect()
        except Exception as e:
            print(f"Telemetry logging error: {e}")
    
    def get_dataset_lineage(self, dataset_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get upstream and downstream lineage for a dataset"""
        # Upstream lineage
        upstream_sql = """
        WITH RECURSIVE lineage_cte (node_id, level, path) AS (
            SELECT ln.NODE_ID, 0 as level, ln.NAME as path
            FROM GOV_PLATFORM.LINEAGE.LINEAGE_NODE ln
            WHERE ln.REF_ID = ? AND ln.NODE_TYPE = 'DATASET'
            
            UNION ALL
            
            SELECT le.SRC_NODE_ID, lc.level + 1, lc.path || ' <- ' || ln.NAME
            FROM lineage_cte lc
            JOIN GOV_PLATFORM.LINEAGE.LINEAGE_EDGE le ON le.TGT_NODE_ID = lc.node_id
            JOIN GOV_PLATFORM.LINEAGE.LINEAGE_NODE ln ON ln.NODE_ID = le.SRC_NODE_ID
            WHERE lc.level < ?
        )
        SELECT * FROM lineage_cte WHERE level > 0 ORDER BY level, path
        """
        
        # Downstream lineage  
        downstream_sql = """
        WITH RECURSIVE lineage_cte (node_id, level, path) AS (
            SELECT ln.NODE_ID, 0 as level, ln.NAME as path
            FROM GOV_PLATFORM.LINEAGE.LINEAGE_NODE ln
            WHERE ln.REF_ID = ? AND ln.NODE_TYPE = 'DATASET'
            
            UNION ALL
            
            SELECT le.TGT_NODE_ID, lc.level + 1, lc.path || ' -> ' || ln.NAME
            FROM lineage_cte lc
            JOIN GOV_PLATFORM.LINEAGE.LINEAGE_EDGE le ON le.SRC_NODE_ID = lc.node_id
            JOIN GOV_PLATFORM.LINEAGE.LINEAGE_NODE ln ON ln.NODE_ID = le.TGT_NODE_ID
            WHERE lc.level < ?
        )
        SELECT * FROM lineage_cte WHERE level > 0 ORDER BY level, path
        """
        
        upstream_df = self.execute_query(upstream_sql, [dataset_id, depth])
        downstream_df = self.execute_query(downstream_sql, [dataset_id, depth])
        
        return {
            'upstream': upstream_df.to_dict('records'),
            'downstream': downstream_df.to_dict('records')
        }
    
    def search_datasets(self, search_term: str, limit: int = 100) -> pd.DataFrame:
        """Search datasets by name or description"""
        sql = """
        SELECT ds.DATASET_ID, ds.DATABASE_NAME, ds.SCHEMA_NAME, ds.OBJECT_NAME,
               ds.DESCRIPTION, ds.CLASSIFICATION, ds.CERTIFICATION, 
               d.DOMAIN_NAME, ds.CREATED_AT
        FROM GOV_PLATFORM.CATALOG.DIM_DATASET ds
        LEFT JOIN GOV_PLATFORM.CATALOG.DIM_DOMAIN d ON d.DOMAIN_ID = ds.DOMAIN_ID
        WHERE ds.OBJECT_NAME ILIKE ? 
           OR ds.DESCRIPTION ILIKE ?
           OR d.DOMAIN_NAME ILIKE ?
        ORDER BY ds.OBJECT_NAME
        LIMIT ?
        """
        
        search_pattern = f"%{search_term}%"
        return self.execute_query(sql, [search_pattern, search_pattern, search_pattern, limit])


# Global DAL instance
dal = GovernanceDAL()

# Convenience functions for backward compatibility
def today_health(domain=None, process=None):
    return dal.today_health(domain, process)

def dq_results(limit=500, domain=None, severity=None, outcome=None):
    return dal.dq_results(limit, domain, severity, outcome)

def control_results(limit=500, control_type=None, outcome=None):
    return dal.control_results(limit, control_type, outcome)

def dataset_owners(domain=None):
    return dal.dataset_owners(domain)

def lineage_edges(node_name=None, active_only=True):
    return dal.lineage_edges(node_name, active_only)

def business_glossary(search_term=None, domain=None):
    return dal.business_glossary(search_term, domain)

def data_contracts(status=None, domain=None):
    return dal.data_contracts(status, domain)

def risk_dashboard(category=None, severity=None):
    return dal.risk_dashboard(category, severity)

def get_domains():
    return dal.get_domains()

def get_processes():
    return dal.get_processes()

def log_telemetry(user_name, role_name, page_name, action, duration_ms=0, 
                 query_count=0, filters=None, error_message=None):
    return dal.log_telemetry(user_name, role_name, page_name, action, 
                           duration_ms, query_count, filters, error_message)
