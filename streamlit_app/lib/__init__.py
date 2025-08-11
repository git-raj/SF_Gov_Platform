"""
Governance Platform Library Package
Provides utility modules for the Streamlit application
"""

# Make imports available at package level
from .dal import dal, today_health, dq_results, control_results
from .authz import get_current_user_role, has_access, page_visible
from .filters import get_domains, get_processes, create_domain_filter
from .charts import create_outcome_pie_chart, create_trend_line_chart

__version__ = "1.0.0"
__author__ = "Governance Platform Team"

__all__ = [
    # Data Access Layer
    'dal',
    'today_health',
    'dq_results', 
    'control_results',
    
    # Authorization
    'get_current_user_role',
    'has_access',
    'page_visible',
    
    # Filters
    'get_domains',
    'get_processes', 
    'create_domain_filter',
    
    # Charts
    'create_outcome_pie_chart',
    'create_trend_line_chart'
]
