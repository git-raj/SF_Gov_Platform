"""
Authorization utilities for Snowflake Governance Platform
Provides role-based access control and permission checking
"""

from snowflake.snowpark.context import get_active_session
from typing import Optional, Dict, Any, List
import streamlit as st
from .dal import dal

def get_current_user_role() -> tuple:
    """Get current user and role from Snowpark session"""
    try:
        session = get_active_session()
        user = session.get_current_user()
        role = session.get_current_role()
        return user, role
    except Exception as e:
        st.error(f"Error getting user context: {e}")
        return "UNKNOWN", "PUBLIC"

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_user_permissions(role_name: str) -> Dict[str, Any]:
    """Get comprehensive permissions for a role"""
    try:
        # Get feature flags
        feature_flags = dal.get_feature_flags(role_name)
        
        # Get page access levels
        page_access_sql = """
        SELECT PAGE_NAME, ACCESS_LEVEL, DESCRIPTION
        FROM GOV_APP.CONFIG.ROLE_PAGE_ACCESS 
        WHERE ROLE_NAME = ?
        """
        page_access_df = dal.execute_query(page_access_sql, [role_name])
        page_access = {}
        for _, row in page_access_df.iterrows():
            page_access[row['PAGE_NAME']] = {
                'level': row['ACCESS_LEVEL'],
                'description': row['DESCRIPTION']
            }
        
        # Get domain visibility
        domain_access_sql = """
        SELECT DOMAIN_NAME, ACCESS_TYPE, RESTRICTIONS
        FROM GOV_APP.CONFIG.DOMAIN_VISIBILITY 
        WHERE ROLE_NAME = ?
        """
        domain_access_df = dal.execute_query(domain_access_sql, [role_name])
        domain_access = {}
        for _, row in domain_access_df.iterrows():
            domain_access[row['DOMAIN_NAME']] = {
                'type': row['ACCESS_TYPE'],
                'restrictions': row['RESTRICTIONS']
            }
        
        return {
            'feature_flags': feature_flags,
            'page_access': page_access,
            'domain_access': domain_access
        }
    except Exception as e:
        st.error(f"Error getting permissions for role {role_name}: {e}")
        return {'feature_flags': {}, 'page_access': {}, 'domain_access': {}}

def page_visible(page_name: str, required_level: str = "READ") -> bool:
    """Check if current user can access a page"""
    try:
        _, role_name = get_current_user_role()
        access_level = dal.check_page_access(role_name, page_name)
        
        if not access_level:
            return False
        
        # Access level hierarchy: ADMIN > WRITE > READ
        level_hierarchy = {"READ": 1, "WRITE": 2, "ADMIN": 3}
        
        user_level = level_hierarchy.get(access_level, 0)
        required_level_val = level_hierarchy.get(required_level, 1)
        
        return user_level >= required_level_val
    except Exception as e:
        st.error(f"Error checking page access: {e}")
        return False

def has_access(permission: str) -> bool:
    """Check if current user has specific permission"""
    try:
        _, role_name = get_current_user_role()
        
        # Define permission mappings
        permission_mappings = {
            "EXPORT": ["GOVERNANCE_ADMIN", "DATA_STEWARD", "GOVERNANCE_ANALYST"],
            "ADMIN": ["GOVERNANCE_ADMIN"],
            "AUDIT": ["GOVERNANCE_ADMIN", "AUDIT_ROLE"],
            "RISK_MGMT": ["GOVERNANCE_ADMIN", "RISK_MANAGER", "AUDIT_ROLE"],
            "STEWARD": ["GOVERNANCE_ADMIN", "DATA_STEWARD"],
            "ANALYST": ["GOVERNANCE_ADMIN", "DATA_STEWARD", "GOVERNANCE_ANALYST"],
            "VIEW_PII": ["GOVERNANCE_ADMIN", "DATA_STEWARD", "PRIVACY_OFFICER"],
            "MODIFY_GLOSSARY": ["GOVERNANCE_ADMIN", "DATA_STEWARD"],
            "APPROVE_CHANGES": ["GOVERNANCE_ADMIN", "DATA_STEWARD"],
            "CREATE_TICKETS": ["GOVERNANCE_ADMIN", "DATA_STEWARD", "GOVERNANCE_ANALYST"]
        }
        
        allowed_roles = permission_mappings.get(permission, [])
        return role_name in allowed_roles
        
    except Exception as e:
        st.error(f"Error checking permission {permission}: {e}")
        return False

def can_access_domain(domain_name: str) -> bool:
    """Check if current user can access a specific domain"""
    try:
        _, role_name = get_current_user_role()
        domain_access = dal.check_domain_access(role_name, domain_name)
        
        if not domain_access:
            # Default deny if no explicit permission
            return False
        
        return domain_access.get('ACCESS_TYPE') in ['FULL', 'LIMITED']
        
    except Exception as e:
        st.error(f"Error checking domain access: {e}")
        return False

def get_accessible_domains() -> List[str]:
    """Get list of domains accessible to current user"""
    try:
        _, role_name = get_current_user_role()
        
        sql = """
        SELECT DOMAIN_NAME 
        FROM GOV_APP.CONFIG.DOMAIN_VISIBILITY 
        WHERE ROLE_NAME = ? AND ACCESS_TYPE IN ('FULL', 'LIMITED')
        ORDER BY DOMAIN_NAME
        """
        
        df = dal.execute_query(sql, [role_name])
        return df['DOMAIN_NAME'].tolist() if not df.empty else []
        
    except Exception as e:
        st.error(f"Error getting accessible domains: {e}")
        return []

def feature_enabled(feature_name: str) -> bool:
    """Check if a feature is enabled for current user"""
    try:
        _, role_name = get_current_user_role()
        feature_flags = dal.get_feature_flags(role_name)
        
        feature = feature_flags.get(feature_name, {})
        return feature.get('enabled', False)
        
    except Exception as e:
        st.error(f"Error checking feature {feature_name}: {e}")
        return False

def get_feature_config(feature_name: str) -> Dict[str, Any]:
    """Get configuration for a specific feature"""
    try:
        _, role_name = get_current_user_role()
        feature_flags = dal.get_feature_flags(role_name)
        
        feature = feature_flags.get(feature_name, {})
        return feature.get('config', {})
        
    except Exception as e:
        st.error(f"Error getting feature config for {feature_name}: {e}")
        return {}

def require_permission(permission: str):
    """Decorator/function to require specific permission"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not has_access(permission):
                st.error(f"🚫 Access Denied: Required permission '{permission}' not found for your role.")
                st.stop()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def require_page_access(page_name: str, level: str = "READ"):
    """Decorator/function to require page access"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not page_visible(page_name, level):
                st.error(f"🚫 Access Denied: You don't have {level} access to {page_name} page.")
                st.stop()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def show_access_denied(message: str = None):
    """Display access denied message"""
    default_message = "🚫 Access Denied: You don't have permission to perform this action."
    st.error(message or default_message)
    
    user, role = get_current_user_role()
    st.info(f"Current user: {user} | Current role: {role}")
    
    with st.expander("Need access? Here's how to request it:"):
        st.markdown("""
        **To request access:**
        1. Contact your Governance Administrator
        2. Specify the page/feature you need access to
        3. Provide business justification
        4. Your request will be reviewed and approved if appropriate
        
        **Contact Information:**
        - Governance Team: governance@yourcompany.com
        - Help Desk: Create a ticket in ServiceNow
        """)

def log_access_attempt(page_name: str, action: str, success: bool, reason: str = None):
    """Log access attempts for audit purposes"""
    try:
        user, role = get_current_user_role()
        
        status = "SUCCESS" if success else "DENIED"
        error_message = reason if not success else None
        
        dal.log_telemetry(
            user_name=user,
            role_name=role, 
            page_name=page_name,
            action=f"ACCESS_ATTEMPT_{action}",
            error_message=error_message
        )
        
        # Also log to dedicated access log if needed
        access_log_sql = """
        INSERT INTO GOV_APP.CONFIG.ACCESS_LOG 
        (USER_NAME, ROLE_NAME, PAGE_NAME, ACTION, STATUS, REASON, TIMESTAMP_TZ)
        SELECT ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()
        WHERE EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_SCHEMA = 'GOV_APP.CONFIG' AND TABLE_NAME = 'ACCESS_LOG')
        """
        
        try:
            dal.execute_query(access_log_sql, [user, role, page_name, action, status, reason])
        except:
            pass  # Table might not exist, that's okay
            
    except Exception as e:
        # Don't fail the main operation if logging fails
        print(f"Failed to log access attempt: {e}")

class PermissionChecker:
    """Context manager for checking permissions"""
    
    def __init__(self, required_permission: str = None, page_name: str = None, access_level: str = "READ"):
        self.required_permission = required_permission
        self.page_name = page_name
        self.access_level = access_level
        self.user, self.role = get_current_user_role()
    
    def __enter__(self):
        # Check permissions on entry
        if self.required_permission and not has_access(self.required_permission):
            log_access_attempt(self.page_name or "UNKNOWN", 
                             f"PERMISSION_{self.required_permission}", 
                             False, f"Missing permission: {self.required_permission}")
            show_access_denied(f"Required permission: {self.required_permission}")
            st.stop()
        
        if self.page_name and not page_visible(self.page_name, self.access_level):
            log_access_attempt(self.page_name, f"PAGE_ACCESS_{self.access_level}", 
                             False, f"Insufficient access level")
            show_access_denied(f"Required access level: {self.access_level} for page: {self.page_name}")
            st.stop()
        
        # Log successful access
        if self.page_name:
            log_access_attempt(self.page_name, f"PAGE_ACCESS_{self.access_level}", True)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up if needed
        pass
    
    def has_permission(self, permission: str) -> bool:
        """Check additional permission within context"""
        return has_access(permission)
    
    def can_modify(self) -> bool:
        """Check if user can modify data"""
        return self.access_level in ["WRITE", "ADMIN"] or has_access("STEWARD")

def create_role_info_display():
    """Create a sidebar widget showing current role info"""
    user, role = get_current_user_role()
    
    with st.sidebar:
        st.markdown("---")
        st.markdown("**👤 User Context**")
        st.text(f"User: {user}")
        st.text(f"Role: {role}")
        
        # Show accessible domains
        domains = get_accessible_domains()
        if domains:
            with st.expander("📂 Accessible Domains"):
                for domain in domains:
                    st.text(f"• {domain}")
        
        # Show enabled features
        feature_flags = dal.get_feature_flags(role)
        enabled_features = [name for name, config in feature_flags.items() if config.get('enabled')]
        
        if enabled_features:
            with st.expander("🔧 Enabled Features"):
                for feature in enabled_features:
                    st.text(f"• {feature.replace('_', ' ').title()}")

def check_and_display_permissions(page_name: str, required_level: str = "READ") -> bool:
    """Check permissions and display appropriate UI"""
    if not page_visible(page_name, required_level):
        show_access_denied()
        return False
    
    return True
