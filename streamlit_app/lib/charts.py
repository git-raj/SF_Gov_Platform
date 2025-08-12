import altair as alt
import pandas as pd

def kpi_card(container, title: str, value):
    container.metric(title, f"{value}")

def outcome_chart(df: pd.DataFrame):
    if df is None or len(df) == 0:
        # Empty chart placeholder
        return alt.Chart(pd.DataFrame({'x':[0], 'y':[0]})).mark_point().encode(x='x', y='y').properties(height=50)

    # Prefer completeness_rate or validity_rate if present
    metric = None
    for cand in ("completeness_rate", "validity_rate"):
        if cand in df.columns:
            metric = cand
            break

    if metric:
        base = alt.Chart(df).mark_bar().encode(
            x=alt.X('RULE_ID:N', sort=None, title='Rule'),
            y=alt.Y(f'{metric}:Q', title=metric.replace('_',' ').title()),
            color=alt.Color('OUTCOME:N')
        ).properties(height=300)
        return base
    else:
        # fallback: count by OUTCOME
        agg = df.groupby('OUTCOME').size().reset_index(name='COUNT')
        return alt.Chart(agg).mark_bar().encode(
            x=alt.X('OUTCOME:N', title='Outcome'),
            y=alt.Y('COUNT:Q', title='Count')
        ).properties(height=300)
