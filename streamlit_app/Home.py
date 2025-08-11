"""
Snowflake Governance Platform - Main Dashboard
Home page displaying today's health metrics and key governance indicators
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from lib import dal, filters, authz, charts
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Governance & Data Quality Platform",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session
session = get_active_session()

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .success-metric {
        border-left-color: #2ca02c;
    }
    .warning-metric {
        border-left-color: #ff7f0e;
    }
    .error-metric {
        border-left-color: #d62728;
    }
    .stSelectbox > div > div {
        background-color: white;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🎯 Governance & Data Quality Platform")
st.markdown("Real-time monitoring of data governance, quality, and compliance across the enterprise")

# Sidebar filters
st.sidebar.header("Filters")

# Domain filter
available_domains = filters.get_domains()
selected_domain = st.sidebar.selectbox(
    "Domain",
    options=["All"] + available_domains,
    index=0,
    help="Filter data by business domain"
)
domain_filter = None if selected_domain == "All" else selected_domain

# Date range filter
date_range = st.sidebar.date_input(
    "Date Range",
    value=(datetime.now().date() - timedelta(days=7), datetime.now().date()),
    help="Select date range for historical analysis"
)

# Process filter
available_processes = filters.get_processes()
selected_process = st.sidebar.selectbox(
    "Process",
    options=["All"] + available_processes,
    index=0,
    help="Filter by specific data process"
)
process_filter = None if selected_process == "All" else selected_process

# Check user permissions
user_role = session.get_current_role()
user_name = session.get_current_user()

if not authz.page_visible("HOME"):
    st.error("🚫 Access Denied: You don't have permission to view this page.")
    st.stop()

# Main dashboard content
col1, col2, col3, col4 = st.columns(4)

# Get today's health data
today_health_df = dal.today_health(domain=domain_filter, process=process_filter)

if not today_health_df.empty:
    # Calculate KPIs
    total_runs = len(today_health_df)
    pass_count = len(today_health_df[today_health_df['OUTCOME'] == 'PASS'])
    warn_count = len(today_health_df[today_health_df['OUTCOME'] == 'WARN'])
    fail_count = len(today_health_df[today_health_df['OUTCOME'] == 'FAIL'])
    
    pass_rate = (pass_count / total_runs * 100) if total_runs > 0 else 0
    
    # Display KPIs
    with col1:
        st.markdown(f"""
        <div class="metric-card success-metric">
            <h3 style="margin:0; color: #2ca02c;">✅ Passed</h3>
            <h1 style="margin:0; font-size: 2.5rem;">{pass_count}</h1>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card warning-metric">
            <h3 style="margin:0; color: #ff7f0e;">⚠️ Warnings</h3>
            <h1 style="margin:0; font-size: 2.5rem;">{warn_count}</h1>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card error-metric">
            <h3 style="margin:0; color: #d62728;">❌ Failed</h3>
            <h1 style="margin:0; font-size: 2.5rem;">{fail_count}</h1>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="margin:0; color: #1f77b4;">📊 Pass Rate</h3>
            <h1 style="margin:0; font-size: 2.5rem;">{pass_rate:.1f}%</h1>
        </div>
        """, unsafe_allow_html=True)

    # Charts section
    st.markdown("---")
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("📈 Today's Results by Domain")
        
        if 'DOMAIN_NAME' in today_health_df.columns:
            domain_summary = today_health_df.groupby(['DOMAIN_NAME', 'OUTCOME']).size().reset_index(name='COUNT')
            
            if not domain_summary.empty:
                fig_domain = px.bar(
                    domain_summary,
                    x='DOMAIN_NAME',
                    y='COUNT',
                    color='OUTCOME',
                    color_discrete_map={
                        'PASS': '#2ca02c',
                        'WARN': '#ff7f0e', 
                        'FAIL': '#d62728'
                    },
                    title="Results by Business Domain"
                )
                fig_domain.update_layout(
                    xaxis_title="Domain",
                    yaxis_title="Count",
                    showlegend=True
                )
                st.plotly_chart(fig_domain, use_container_width=True)
            else:
                st.info("No domain data available for the selected filters.")
        else:
            st.info("Domain information not available in today's data.")
    
    with chart_col2:
        st.subheader("⏰ Process Duration Analysis")
        
        if 'DURATION_MINUTES' in today_health_df.columns:
            duration_data = today_health_df[today_health_df['DURATION_MINUTES'].notna()]
            
            if not duration_data.empty:
                fig_duration = px.box(
                    duration_data,
                    x='OUTCOME',
                    y='DURATION_MINUTES',
                    color='OUTCOME',
                    color_discrete_map={
                        'PASS': '#2ca02c',
                        'WARN': '#ff7f0e',
                        'FAIL': '#d62728'
                    },
                    title="Process Duration by Outcome"
                )
                fig_duration.update_layout(
                    xaxis_title="Outcome",
                    yaxis_title="Duration (Minutes)",
                    showlegend=False
                )
                st.plotly_chart(fig_duration, use_container_width=True)
            else:
                st.info("No duration data available.")
        else:
            st.info("Duration information not available.")
    
    # Detailed results table
    st.markdown("---")
    st.subheader("📋 Today's Detailed Results")
    
    # Add filters for the table
    table_col1, table_col2, table_col3 = st.columns(3)
    
    with table_col1:
        outcome_filter = st.selectbox(
            "Filter by Outcome",
            options=["All", "PASS", "WARN", "FAIL"],
            key="outcome_filter"
        )
    
    with table_col2:
        min_duration = st.number_input(
            "Min Duration (minutes)",
            min_value=0,
            value=0,
            key="min_duration"
        )
    
    with table_col3:
        show_owners = st.checkbox("Show Owners", value=True)
    
    # Filter the data
    filtered_df = today_health_df.copy()
    
    if outcome_filter != "All":
        filtered_df = filtered_df[filtered_df['OUTCOME'] == outcome_filter]
    
    if min_duration > 0 and 'DURATION_MINUTES' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['DURATION_MINUTES'] >= min_duration]
    
    # Select columns to display
    display_columns = ['RUN_ID', 'PROCESS_NAME', 'TARGET_TABLE', 'OUTCOME', 'STARTED_AT', 'DURATION_MINUTES']
    
    if 'DOMAIN_NAME' in filtered_df.columns:
        display_columns.insert(2, 'DOMAIN_NAME')
    
    if show_owners and 'OWNERS' in filtered_df.columns:
        display_columns.append('OWNERS')
    
    # Display the table
    if not filtered_df.empty:
        display_df = filtered_df[display_columns].copy()
        
        # Format the display
        if 'STARTED_AT' in display_df.columns:
            display_df['STARTED_AT'] = pd.to_datetime(display_df['STARTED_AT']).dt.strftime('%H:%M:%S')
        
        if 'DURATION_MINUTES' in display_df.columns:
            display_df['DURATION_MINUTES'] = display_df['DURATION_MINUTES'].round(1)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "OUTCOME": st.column_config.TextColumn(
                    "Outcome",
                    help="Process execution outcome"
                ),
                "DURATION_MINUTES": st.column_config.NumberColumn(
                    "Duration (min)",
                    help="Process execution duration in minutes",
                    format="%.1f"
                )
            }
        )
        
        # Export functionality
        if authz.has_access("EXPORT"):
            csv_data = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv_data,
                file_name=f"governance_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No results match the selected filters.")

else:
    st.warning("No data available for today. Check your filters or data pipeline status.")

# Quick actions section
st.markdown("---")
st.subheader("🚀 Quick Actions")

action_col1, action_col2, action_col3, action_col4 = st.columns(4)

with action_col1:
    if st.button("📊 View DQ Explorer", use_container_width=True):
        st.switch_page("pages/1_🔍_DQ_Explorer.py")

with action_col2:
    if st.button("🧭 Check Lineage", use_container_width=True):
        st.switch_page("pages/2_🧭_Lineage.py")

with action_col3:
    if st.button("👤 Review Ownership", use_container_width=True):
        st.switch_page("pages/3_👤_Ownership.py")

with action_col4:
    if st.button("📜 View Policies", use_container_width=True):
        st.switch_page("pages/4_📜_Policies.py")

# Footer with refresh info
st.markdown("---")
refresh_col1, refresh_col2 = st.columns([3, 1])

with refresh_col1:
    st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | User: {user_name} | Role: {user_role}")

with refresh_col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# Log telemetry
dal.log_telemetry(
    user_name=user_name,
    role_name=user_role,
    page_name="HOME",
    action="VIEW",
    filters={
        "domain": domain_filter,
        "process": process_filter,
        "date_range": [str(d) for d in date_range]
    }
)
