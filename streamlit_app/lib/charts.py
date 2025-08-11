"""
Chart utilities for Snowflake Governance Platform
Provides reusable chart components and visualization helpers
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta

# Color palette for consistent styling
COLORS = {
    'primary': '#1f77b4',
    'success': '#2ca02c', 
    'warning': '#ff7f0e',
    'danger': '#d62728',
    'info': '#17becf',
    'secondary': '#7f7f7f',
    'purple': '#9467bd',
    'brown': '#8c564b',
    'pink': '#e377c2',
    'olive': '#bcbd22'
}

OUTCOME_COLORS = {
    'PASS': COLORS['success'],
    'WARN': COLORS['warning'],
    'FAIL': COLORS['danger'],
    'SUCCESS': COLORS['success'],
    'FAILED': COLORS['danger'],
    'PARTIAL': COLORS['warning']
}

SEVERITY_COLORS = {
    'CRITICAL': COLORS['danger'],
    'HIGH': '#ff4444',
    'MEDIUM': COLORS['warning'], 
    'LOW': '#ffd700'
}

def create_outcome_pie_chart(df: pd.DataFrame, outcome_col: str = 'OUTCOME', 
                           title: str = "Results Distribution") -> go.Figure:
    """Create a pie chart for outcome distribution"""
    if df.empty or outcome_col not in df.columns:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    outcome_counts = df[outcome_col].value_counts()
    
    fig = px.pie(
        values=outcome_counts.values,
        names=outcome_counts.index,
        title=title,
        color=outcome_counts.index,
        color_discrete_map=OUTCOME_COLORS
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=400,
        font=dict(size=12)
    )
    
    return fig

def create_trend_line_chart(df: pd.DataFrame, date_col: str, value_col: str,
                          group_col: str = None, title: str = "Trend Analysis") -> go.Figure:
    """Create a line chart showing trends over time"""
    if df.empty or date_col not in df.columns or value_col not in df.columns:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper", 
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    # Ensure date column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col])
    
    if group_col and group_col in df.columns:
        fig = px.line(
            df, x=date_col, y=value_col, color=group_col,
            title=title,
            color_discrete_sequence=list(COLORS.values())
        )
    else:
        fig = px.line(df, x=date_col, y=value_col, title=title)
    
    fig.update_layout(
        xaxis_title=date_col.replace('_', ' ').title(),
        yaxis_title=value_col.replace('_', ' ').title(),
        hovermode='x unified',
        height=400
    )
    
    return fig

def create_stacked_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, 
                           color_col: str, title: str = "Stacked Bar Chart") -> go.Figure:
    """Create a stacked bar chart"""
    if df.empty or not all(col in df.columns for col in [x_col, y_col, color_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.bar(
        df, x=x_col, y=y_col, color=color_col,
        title=title,
        color_discrete_map=OUTCOME_COLORS
    )
    
    fig.update_layout(
        xaxis_title=x_col.replace('_', ' ').title(),
        yaxis_title=y_col.replace('_', ' ').title(),
        height=400,
        showlegend=True
    )
    
    return fig

def create_gauge_chart(value: float, max_value: float = 100, 
                      title: str = "Gauge Chart", 
                      thresholds: Dict[str, float] = None) -> go.Figure:
    """Create a gauge chart for KPIs"""
    if thresholds is None:
        thresholds = {'red': 50, 'yellow': 75, 'green': 100}
    
    # Determine color based on thresholds
    if value < thresholds['red']:
        color = COLORS['danger']
    elif value < thresholds['yellow']:
        color = COLORS['warning']
    else:
        color = COLORS['success']
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, thresholds['red']], 'color': "lightgray"},
                {'range': [thresholds['red'], thresholds['yellow']], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': thresholds['yellow']
            }
        }
    ))
    
    fig.update_layout(height=300)
    return fig

def create_heatmap(df: pd.DataFrame, x_col: str, y_col: str, value_col: str,
                  title: str = "Heatmap") -> go.Figure:
    """Create a heatmap visualization"""
    if df.empty or not all(col in df.columns for col in [x_col, y_col, value_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    # Pivot the data for heatmap
    heatmap_data = df.pivot_table(values=value_col, index=y_col, columns=x_col, aggfunc='sum')
    
    fig = px.imshow(
        heatmap_data,
        title=title,
        color_continuous_scale='RdYlBu_r',
        aspect='auto'
    )
    
    fig.update_layout(
        xaxis_title=x_col.replace('_', ' ').title(),
        yaxis_title=y_col.replace('_', ' ').title(),
        height=400
    )
    
    return fig

def create_box_plot(df: pd.DataFrame, x_col: str, y_col: str,
                   title: str = "Box Plot Analysis") -> go.Figure:
    """Create a box plot for distribution analysis"""
    if df.empty or not all(col in df.columns for col in [x_col, y_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.box(
        df, x=x_col, y=y_col,
        title=title,
        color=x_col,
        color_discrete_map=OUTCOME_COLORS
    )
    
    fig.update_layout(
        xaxis_title=x_col.replace('_', ' ').title(),
        yaxis_title=y_col.replace('_', ' ').title(),
        height=400,
        showlegend=False
    )
    
    return fig

def create_scatter_plot(df: pd.DataFrame, x_col: str, y_col: str, 
                       color_col: str = None, size_col: str = None,
                       title: str = "Scatter Plot") -> go.Figure:
    """Create a scatter plot for correlation analysis"""
    if df.empty or not all(col in df.columns for col in [x_col, y_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.scatter(
        df, x=x_col, y=y_col, 
        color=color_col if color_col and color_col in df.columns else None,
        size=size_col if size_col and size_col in df.columns else None,
        title=title,
        color_discrete_sequence=list(COLORS.values())
    )
    
    fig.update_layout(
        xaxis_title=x_col.replace('_', ' ').title(),
        yaxis_title=y_col.replace('_', ' ').title(),
        height=400
    )
    
    return fig

def create_histogram(df: pd.DataFrame, col: str, bins: int = 20,
                    title: str = "Distribution Analysis") -> go.Figure:
    """Create a histogram for distribution analysis"""
    if df.empty or col not in df.columns:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.histogram(
        df, x=col, nbins=bins,
        title=title,
        color_discrete_sequence=[COLORS['primary']]
    )
    
    fig.update_layout(
        xaxis_title=col.replace('_', ' ').title(),
        yaxis_title="Frequency",
        height=400
    )
    
    return fig

def create_metric_cards(metrics: Dict[str, Any], cols: int = 4) -> None:
    """Create a row of metric cards"""
    if not metrics:
        st.info("No metrics available")
        return
    
    columns = st.columns(cols)
    
    for i, (label, data) in enumerate(metrics.items()):
        col_idx = i % cols
        
        with columns[col_idx]:
            if isinstance(data, dict):
                value = data.get('value', 0)
                delta = data.get('delta', None)
                delta_color = data.get('delta_color', 'normal')
                help_text = data.get('help', None)
                
                st.metric(
                    label=label,
                    value=value,
                    delta=delta,
                    delta_color=delta_color,
                    help=help_text
                )
            else:
                st.metric(label=label, value=data)

def create_sunburst_chart(df: pd.DataFrame, path_cols: List[str], 
                         value_col: str, title: str = "Hierarchy View") -> go.Figure:
    """Create a sunburst chart for hierarchical data"""
    if df.empty or not all(col in df.columns for col in path_cols + [value_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.sunburst(
        df, path=path_cols, values=value_col,
        title=title,
        color_discrete_sequence=list(COLORS.values())
    )
    
    fig.update_layout(height=500)
    return fig

def create_waterfall_chart(categories: List[str], values: List[float],
                          title: str = "Waterfall Chart") -> go.Figure:
    """Create a waterfall chart for showing changes"""
    if not categories or not values or len(categories) != len(values):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = go.Figure(go.Waterfall(
        name="", orientation="v",
        measure=["relative"] * (len(values) - 1) + ["total"],
        x=categories,
        textposition="outside",
        text=[f"+{v}" if v > 0 else str(v) for v in values],
        y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
    ))
    
    fig.update_layout(
        title=title,
        showlegend=False,
        height=400
    )
    
    return fig

def create_treemap(df: pd.DataFrame, path_cols: List[str], value_col: str,
                  title: str = "Treemap View") -> go.Figure:
    """Create a treemap for hierarchical data visualization"""
    if df.empty or not all(col in df.columns for col in path_cols + [value_col]):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = px.treemap(
        df, path=path_cols, values=value_col,
        title=title,
        color_discrete_sequence=list(COLORS.values())
    )
    
    fig.update_layout(height=500)
    return fig

def create_funnel_chart(stages: List[str], values: List[int],
                       title: str = "Funnel Analysis") -> go.Figure:
    """Create a funnel chart for process analysis"""
    if not stages or not values or len(stages) != len(values):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textposition="inside",
        opacity=0.65,
        marker={
            "color": list(COLORS.values())[:len(stages)],
            "line": {"width": [2] * len(stages), "color": "wheat"}
        }
    ))
    
    fig.update_layout(
        title=title,
        height=400
    )
    
    return fig

def create_radar_chart(categories: List[str], values: List[float],
                      title: str = "Radar Chart", max_value: float = 100) -> go.Figure:
    """Create a radar chart for multi-dimensional analysis"""
    if not categories or not values or len(categories) != len(values):
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle'
        )
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='Values',
        line_color=COLORS['primary']
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max_value]
            )),
        showlegend=False,
        title=title,
        height=400
    )
    
    return fig

class ChartBuilder:
    """Utility class for building charts with consistent styling"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.colors = COLORS
        
    def outcome_distribution(self, outcome_col: str = 'OUTCOME') -> go.Figure:
        """Build outcome distribution pie chart"""
        return create_outcome_pie_chart(self.df, outcome_col)
    
    def trend_analysis(self, date_col: str, value_col: str, group_col: str = None) -> go.Figure:
        """Build trend line chart"""
        return create_trend_line_chart(self.df, date_col, value_col, group_col)
    
    def domain_performance(self, domain_col: str = 'DOMAIN_NAME', 
                          outcome_col: str = 'OUTCOME') -> go.Figure:
        """Build domain performance stacked bar chart"""
        if self.df.empty or not all(col in self.df.columns for col in [domain_col, outcome_col]):
            return go.Figure()
        
        summary_df = self.df.groupby([domain_col, outcome_col]).size().reset_index(name='COUNT')
        return create_stacked_bar_chart(summary_df, domain_col, 'COUNT', outcome_col, 
                                       "Performance by Domain")
    
    def duration_analysis(self, duration_col: str = 'DURATION_MINUTES', 
                         outcome_col: str = 'OUTCOME') -> go.Figure:
        """Build duration box plot"""
        return create_box_plot(self.df, outcome_col, duration_col, "Duration Analysis by Outcome")
    
    def severity_heatmap(self, x_col: str, y_col: str, 
                        severity_col: str = 'SEVERITY') -> go.Figure:
        """Build severity heatmap"""
        if self.df.empty or not all(col in self.df.columns for col in [x_col, y_col, severity_col]):
            return go.Figure()
        
        # Convert severity to numeric for heatmap
        severity_map = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3, 'CRITICAL': 4}
        df_copy = self.df.copy()
        df_copy['SEVERITY_NUMERIC'] = df_copy[severity_col].map(severity_map).fillna(0)
        
        return create_heatmap(df_copy, x_col, y_col, 'SEVERITY_NUMERIC', "Severity Heatmap")

def apply_consistent_styling(fig: go.Figure, theme: str = 'plotly_white') -> go.Figure:
    """Apply consistent styling to charts"""
    fig.update_layout(
        template=theme,
        font=dict(family="Arial, sans-serif", size=12),
        title_font=dict(size=16, family="Arial, sans-serif"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(t=60, r=20, b=60, l=60),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def export_chart_data(fig: go.Figure, filename: str = None) -> bytes:
    """Export chart as image or data"""
    if filename and filename.endswith('.html'):
        return fig.to_html(include_plotlyjs='cdn').encode()
    else:
        return fig.to_image(format='png')
