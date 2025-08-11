-- ====================================================================
-- DBT MODEL: GOVERNANCE METRICS
-- ====================================================================
-- Daily governance metrics aggregation for dashboard KPIs

{{ config(
    materialized='table',
    cluster_by=['metric_date', 'domain_name'],
    comment='Daily governance metrics for dashboard display'
) }}

WITH daily_process_metrics AS (
    SELECT
        DATE(pr.started_at) AS metric_date,
        d.domain_name,
        COUNT(*) AS total_runs,
        COUNT(CASE WHEN pr.status = 'SUCCESS' THEN 1 END) AS successful_runs,
        COUNT(CASE WHEN pr.status = 'FAILED' THEN 1 END) AS failed_runs,
        AVG(DATEDIFF('minute', pr.started_at, pr.ended_at)) AS avg_duration_minutes,
        MAX(DATEDIFF('minute', pr.started_at, pr.ended_at)) AS max_duration_minutes
    FROM {{ ref('process_runs') }} pr
    LEFT JOIN {{ ref('processes') }} p ON p.process_id = pr.process_id
    LEFT JOIN {{ ref('datasets') }} ds ON ds.process_id = p.process_id
    LEFT JOIN {{ ref('domains') }} d ON d.domain_id = ds.domain_id
    WHERE pr.started_at >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1, 2
),

daily_quality_metrics AS (
    SELECT
        DATE(dr.created_at) AS metric_date,
        d.domain_name,
        COUNT(*) AS total_dq_tests,
        COUNT(CASE WHEN dr.outcome = 'PASS' THEN 1 END) AS passed_tests,
        COUNT(CASE WHEN dr.outcome = 'FAIL' THEN 1 END) AS failed_tests,
        COUNT(CASE WHEN dr.outcome = 'WARN' THEN 1 END) AS warning_tests
    FROM {{ source('gov_platform', 'dq_result') }} dr
    LEFT JOIN {{ source('gov_platform', 'dim_dataset') }} ds ON ds.dataset_id = dr.dataset_id
    LEFT JOIN {{ source('gov_platform', 'dim_domain') }} d ON d.domain_id = ds.domain_id
    WHERE dr.created_at >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1, 2
),

daily_control_metrics AS (
    SELECT
        DATE(ct.executed_at) AS metric_date,
        d.domain_name,
        COUNT(*) AS total_control_tests,
        COUNT(CASE WHEN ct.outcome = 'PASS' THEN 1 END) AS passed_controls,
        COUNT(CASE WHEN ct.outcome = 'FAIL' THEN 1 END) AS failed_controls,
        COUNT(CASE WHEN ct.outcome = 'WARN' THEN 1 END) AS warning_controls
    FROM {{ source('gov_platform', 'control_test') }} ct
    LEFT JOIN {{ source('gov_platform', 'control_registry') }} cr ON cr.control_id = ct.control_id
    LEFT JOIN {{ source('gov_platform', 'dim_dataset') }} ds ON ds.dataset_id = cr.scope_id AND cr.scope_type = 'DATASET'
    LEFT JOIN {{ source('gov_platform', 'dim_domain') }} d ON d.domain_id = ds.domain_id
    WHERE ct.executed_at >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1, 2
)

SELECT
    COALESCE(pm.metric_date, qm.metric_date, cm.metric_date) AS metric_date,
    COALESCE(pm.domain_name, qm.domain_name, cm.domain_name) AS domain_name,
    
    -- Process metrics
    COALESCE(pm.total_runs, 0) AS total_process_runs,
    COALESCE(pm.successful_runs, 0) AS successful_process_runs,
    COALESCE(pm.failed_runs, 0) AS failed_process_runs,
    CASE 
        WHEN pm.total_runs > 0 THEN (pm.successful_runs * 100.0 / pm.total_runs)
        ELSE NULL 
    END AS process_success_rate_pct,
    pm.avg_duration_minutes,
    pm.max_duration_minutes,
    
    -- Quality metrics
    COALESCE(qm.total_dq_tests, 0) AS total_dq_tests,
    COALESCE(qm.passed_tests, 0) AS passed_dq_tests,
    COALESCE(qm.failed_tests, 0) AS failed_dq_tests,
    COALESCE(qm.warning_tests, 0) AS warning_dq_tests,
    CASE 
        WHEN qm.total_dq_tests > 0 THEN (qm.passed_tests * 100.0 / qm.total_dq_tests)
        ELSE NULL 
    END AS dq_pass_rate_pct,
    
    -- Control metrics
    COALESCE(cm.total_control_tests, 0) AS total_control_tests,
    COALESCE(cm.passed_controls, 0) AS passed_control_tests,
    COALESCE(cm.failed_controls, 0) AS failed_control_tests,
    COALESCE(cm.warning_controls, 0) AS warning_control_tests,
    CASE 
        WHEN cm.total_control_tests > 0 THEN (cm.passed_controls * 100.0 / cm.total_control_tests)
        ELSE NULL 
    END AS control_pass_rate_pct,
    
    -- Overall health score (weighted average)
    CASE 
        WHEN COALESCE(pm.total_runs, 0) + COALESCE(qm.total_dq_tests, 0) + COALESCE(cm.total_control_tests, 0) > 0 THEN
            (COALESCE(pm.successful_runs, 0) * 40 + 
             COALESCE(qm.passed_tests, 0) * 35 + 
             COALESCE(cm.passed_controls, 0) * 25) * 100.0 / 
            (COALESCE(pm.total_runs, 0) * 40 + 
             COALESCE(qm.total_dq_tests, 0) * 35 + 
             COALESCE(cm.total_control_tests, 0) * 25)
        ELSE NULL 
    END AS overall_health_score_pct,
    
    CURRENT_TIMESTAMP() AS created_at

FROM daily_process_metrics pm
FULL OUTER JOIN daily_quality_metrics qm 
    ON pm.metric_date = qm.metric_date AND pm.domain_name = qm.domain_name
FULL OUTER JOIN daily_control_metrics cm 
    ON COALESCE(pm.metric_date, qm.metric_date) = cm.metric_date 
    AND COALESCE(pm.domain_name, qm.domain_name) = cm.domain_name
WHERE COALESCE(pm.metric_date, qm.metric_date, cm.metric_date) IS NOT NULL
ORDER BY metric_date DESC, domain_name
