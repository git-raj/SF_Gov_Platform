"""
Filter utilities for Snowflake Governance Platform
Provides centralized filter logic and helper functions
"""

from snowflake.snowpark.context import get_active_session
from typing import List, Optional, Dict, Any
import streamlit as st
from .dal import dal

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_domains() -> List[str]:
    """Get list of available domains with caching"""
    try:
        return dal.get_domains()
    except Exception as e:
        st.error(f"Error fetching domains: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes  
def get_processes() -> List[str]:
    """Get list of available processes with caching"""
    try:
        return dal.get_processes()
    except Exception as e:
        st.error(f"Error fetching processes: {e}")
        return []

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_systems() -> List[str]:
    """Get list of available systems"""
    try:
        sql = "SELECT DISTINCT SYSTEM_NAME FROM GOV_PLATFORM.CATALOG.DIM_SYSTEM ORDER BY SYSTEM_NAME"
        df = dal.execute_query(sql)
        return df['SYSTEM_NAME'].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error fetching systems: {e}")
        return []

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_classifications() -> List[str]:
    """Get list of available data classifications"""
    try:
        sql = "SELECT DISTINCT CLASS_NAME FROM GOV_PLATFORM.GOVERNANCE.CLASSIFICATION ORDER BY CLASS_NAME"
        df = dal.execute_query(sql)
        return df['CLASS_NAME'].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error fetching classifications: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_rule_types() -> List[str]:
    """Get list of available DQ rule types"""
    try:
        sql = "SELECT DISTINCT RULE_TYPE FROM GOV_PLATFORM.QUALITY.DQ_RULE ORDER BY RULE_TYPE"
        df = dal.execute_query(sql)
        return df['RULE_TYPE'].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error fetching rule types: {e}")
        return []

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_control_types() -> List[str]:
    """Get list of available control types"""
    try:
        sql = "SELECT DISTINCT CONTROL_TYPE FROM GOV_PLATFORM.GOVERNANCE.CONTROL_REGISTRY ORDER BY CONTROL_TYPE"
        df = dal.execute_query(sql)
        return df['CONTROL_TYPE'].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error fetching control types: {e}")
        return []

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_risk_categories() -> List[str]:
    """Get list of available risk categories"""
    try:
        sql = "SELECT DISTINCT CATEGORY FROM GOV_PLATFORM.RISK.RISK_ITEM ORDER BY CATEGORY"
        df = dal.execute_query(sql)
        return df['CATEGORY'].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error fetching risk categories: {e}")
        return []

def create_domain_filter(key: str = "domain_filter", help_text: str = "Filter data by business domain") -> Optional[str]:
    """Create a domain filter widget"""
    domains = get_domains()
    if not domains:
        return None
    
    selected = st.selectbox(
        "Domain",
        options=["All"] + domains,
        index=0,
        key=key,
        help=help_text
    )
    return None if selected == "All" else selected

def create_severity_filter(key: str = "severity_filter", help_text: str = "Filter by severity level") -> Optional[str]:
    """Create a severity filter widget"""
    severities = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    selected = st.selectbox(
        "Severity",
        options=["All"] + severities,
        index=0,
        key=key,
        help=help_text
    )
    return None if selected == "All" else selected

def create_outcome_filter(key: str = "outcome_filter", help_text: str = "Filter by outcome") -> Optional[str]:
    """Create an outcome filter widget"""
    outcomes = ["PASS", "FAIL", "WARN"]
    selected = st.selectbox(
        "Outcome",
        options=["All"] + outcomes,
        index=0,
        key=key,
        help=help_text
    )
    return None if selected == "All" else selected

def create_status_filter(key: str = "status_filter", statuses: List[str] = None, help_text: str = "Filter by status") -> Optional[str]:
    """Create a generic status filter widget"""
    if not statuses:
        statuses = ["ACTIVE", "INACTIVE", "DRAFT", "APPROVED", "DEPRECATED"]
    
    selected = st.selectbox(
        "Status",
        options=["All"] + statuses,
        index=0,
        key=key,
        help=help_text
    )
    return None if selected == "All" else selected

def create_date_range_filter(key: str = "date_range", help_text: str = "Select date range") -> tuple:
    """Create a date range filter widget"""
    from datetime import datetime, timedelta
    
    default_start = datetime.now().date() - timedelta(days=7)
    default_end = datetime.now().date()
    
    return st.date_input(
        "Date Range",
        value=(default_start, default_end),
        key=key,
        help=help_text
    )

def create_multiselect_filter(label: str, options: List[str], key: str, 
                            help_text: str = None, default_all: bool = True) -> List[str]:
    """Create a multiselect filter widget"""
    if not options:
        return []
    
    if default_all:
        default_selection = options
    else:
        default_selection = []
    
    selected = st.multiselect(
        label,
        options=options,
        default=default_selection,
        key=key,
        help=help_text
    )
    
    return selected if selected else options

def create_search_filter(key: str = "search_term", placeholder: str = "Enter search term...") -> Optional[str]:
    """Create a search text input widget"""
    search_term = st.text_input(
        "Search",
        placeholder=placeholder,
        key=key
    )
    return search_term.strip() if search_term.strip() else None

def create_numeric_range_filter(label: str, min_val: float = 0.0, max_val: float = 100.0,
                              key: str = "numeric_range", step: float = 1.0) -> tuple:
    """Create a numeric range slider widget"""
    return st.slider(
        label,
        min_value=min_val,
        max_value=max_val,
        value=(min_val, max_val),
        step=step,
        key=key
    )

def apply_filters_to_dataframe(df, filters: Dict[str, Any]):
    """Apply multiple filters to a DataFrame"""
    filtered_df = df.copy()
    
    for column, filter_value in filters.items():
        if filter_value is None or filter_value == "All":
            continue
            
        if isinstance(filter_value, list):
            if filter_value:  # Only apply if list is not empty
                filtered_df = filtered_df[filtered_df[column].isin(filter_value)]
        elif isinstance(filter_value, tuple) and len(filter_value) == 2:
            # Assume it's a range filter
            min_val, max_val = filter_value
            filtered_df = filtered_df[
                (filtered_df[column] >= min_val) & 
                (filtered_df[column] <= max_val)
            ]
        elif isinstance(filter_value, str):
            if column.endswith("_SEARCH"):
                # Text search filter
                base_column = column.replace("_SEARCH", "")
                if base_column in filtered_df.columns:
                    filtered_df = filtered_df[
                        filtered_df[base_column].str.contains(filter_value, case=False, na=False)
                    ]
            else:
                # Exact match filter
                filtered_df = filtered_df[filtered_df[column] == filter_value]
        else:
            # Direct equality filter
            filtered_df = filtered_df[filtered_df[column] == filter_value]
    
    return filtered_df

def get_filter_summary(filters: Dict[str, Any]) -> str:
    """Generate a summary string of active filters"""
    active_filters = []
    
    for key, value in filters.items():
        if value is not None and value != "All" and value != []:
            if isinstance(value, list):
                if len(value) < 10:  # Only show if reasonable number
                    active_filters.append(f"{key}: {', '.join(map(str, value))}")
                else:
                    active_filters.append(f"{key}: {len(value)} items selected")
            elif isinstance(value, tuple):
                active_filters.append(f"{key}: {value[0]} - {value[1]}")
            else:
                active_filters.append(f"{key}: {value}")
    
    if not active_filters:
        return "No active filters"
    
    return "Active filters: " + " | ".join(active_filters)

class FilterManager:
    """Context manager for handling filter state"""
    
    def __init__(self, page_name: str):
        self.page_name = page_name
        self.filters = {}
        
    def add_domain_filter(self, key: str = None):
        """Add domain filter to manager"""
        filter_key = key or f"{self.page_name}_domain"
        self.filters['domain'] = create_domain_filter(filter_key)
        return self.filters['domain']
    
    def add_severity_filter(self, key: str = None):
        """Add severity filter to manager"""
        filter_key = key or f"{self.page_name}_severity"
        self.filters['severity'] = create_severity_filter(filter_key)
        return self.filters['severity']
    
    def add_outcome_filter(self, key: str = None):
        """Add outcome filter to manager"""
        filter_key = key or f"{self.page_name}_outcome"
        self.filters['outcome'] = create_outcome_filter(filter_key)
        return self.filters['outcome']
    
    def add_date_range_filter(self, key: str = None):
        """Add date range filter to manager"""
        filter_key = key or f"{self.page_name}_date_range"
        self.filters['date_range'] = create_date_range_filter(filter_key)
        return self.filters['date_range']
    
    def add_search_filter(self, key: str = None, placeholder: str = "Search..."):
        """Add search filter to manager"""
        filter_key = key or f"{self.page_name}_search"
        self.filters['search'] = create_search_filter(filter_key, placeholder)
        return self.filters['search']
    
    def get_active_filters(self) -> Dict[str, Any]:
        """Get all active filters"""
        return {k: v for k, v in self.filters.items() if v is not None and v != "All"}
    
    def clear_filters(self):
        """Clear all filters"""
        for key in self.filters.keys():
            if key in st.session_state:
                del st.session_state[key]
        self.filters.clear()
    
    def get_summary(self) -> str:
        """Get filter summary string"""
        return get_filter_summary(self.get_active_filters())
