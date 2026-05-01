-- Performance Tracking and KPI Monitoring Query Template
-- Purpose: Track key performance indicators across facilities with trend analysis
-- Usage: Replace placeholder names with your actual database/table/column names

WITH daily_volumes AS (
    SELECT
        region_code,
        DATE_TRUNC('week', DATE(snapshot_date) + INTERVAL '1 day') - INTERVAL '1 day' AS week_start_date,
        SUM(total_volume) as total_volume
    FROM performance_db.fact_daily_volumes
    WHERE snapshot_date > '2024-12-31 00:00:00'
    GROUP BY 1, 2
),

performance_summary AS (
    SELECT
        t.facility_id,
        DATE(t.snapshot_date) AS date,
        f.country_name,
        f.region_name,
        f.hub_name,
        MAX(d.total_volume) as total_volume,
        SUM(t.total_impact) AS total_impact,
        -- Calculate performance metrics as basis points (bps)
        (SUM(COALESCE(t.labor_impact, 0) + COALESCE(t.capacity_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS execution_bps,
        (SUM(COALESCE(t.event_impact, 0)) / NULLIF(MAX(d.total_volume), 0)) * 10000 AS event_bps,
        (SUM(COALESCE(t.constraint_impact, 0)) / NULLIF(MAX(d.total_volume), 0)) * 10000 AS constraint_bps,
        (SUM(COALESCE(t.unallocated_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS unallocated_bps,
        (SUM(COALESCE(t.complexity_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS complexity_bps,
        (SUM(COALESCE(t.training_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS training_bps,
        (SUM(COALESCE(t.downtime_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS downtime_bps,
        (SUM(COALESCE(t.planning_impact, 0))/ NULLIF(MAX(d.total_volume), 0)) * 10000 AS planning_bps,
        -- Additional KPIs
        AVG(t.efficiency_percentage) AS avg_efficiency,
        SUM(t.error_count) AS total_errors,
        AVG(t.quality_score) AS avg_quality_score,
        SUM(t.processed_units) AS total_processed_units
    FROM performance_db.fact_daily_performance t
    LEFT JOIN daily_volumes d
        ON t.region_code = d.region_code
        AND DATE_TRUNC('week', DATE(t.snapshot_date) + INTERVAL '1 day') - INTERVAL '1 day' = d.week_start_date
    LEFT JOIN reference_db.dim_facilities f
        ON t.facility_id = f.facility_code
    WHERE t.total_impact > 0 AND t.metric_type = 'DAILY'
    GROUP BY 1,2,3,4,5
    ORDER BY 2
),

trend_analysis AS (
    SELECT
        ps.*,
        -- Calculate week-over-week changes
        LAG(ps.execution_bps, 7) OVER (PARTITION BY ps.facility_id ORDER BY ps.date) AS prev_week_execution_bps,
        LAG(ps.avg_efficiency, 7) OVER (PARTITION BY ps.facility_id ORDER BY ps.date) AS prev_week_efficiency,
        LAG(ps.avg_quality_score, 7) OVER (PARTITION BY ps.facility_id ORDER BY ps.date) AS prev_week_quality,
        -- Calculate moving averages (7-day)
        AVG(ps.execution_bps) OVER (PARTITION BY ps.facility_id ORDER BY ps.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d_execution_bps,
        AVG(ps.avg_efficiency) OVER (PARTITION BY ps.facility_id ORDER BY ps.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d_efficiency,
        AVG(ps.avg_quality_score) OVER (PARTITION BY ps.facility_id ORDER BY ps.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d_quality
    FROM performance_summary ps
),

final_metrics AS (
    SELECT
        ta.facility_id,
        ta.date,
        DATE_PART('week', ta.date + INTERVAL '1 day'):: INTEGER AS week_number,
        EXTRACT(YEAR FROM DATE(ta.date)) AS year,
        ta.country_name,
        ta.region_name,
        ta.hub_name,
        -- Volume and Impact
        ta.total_volume,
        ta.total_impact,
        ta.total_processed_units,
        -- Performance BPS metrics
        ta.execution_bps,
        ta.training_bps,
        ta.downtime_bps,
        ta.event_bps,
        ta.constraint_bps,
        ta.unallocated_bps,
        ta.complexity_bps,
        ta.planning_bps,
        -- KPI metrics
        ta.avg_efficiency,
        ta.total_errors,
        ta.avg_quality_score,
        -- Trend indicators
        CASE
            WHEN ta.prev_week_execution_bps IS NOT NULL
            THEN ((ta.execution_bps - ta.prev_week_execution_bps) / NULLIF(ta.prev_week_execution_bps, 0)) * 100
            ELSE NULL
        END AS execution_bps_wow_change_pct,
        CASE
            WHEN ta.prev_week_efficiency IS NOT NULL
            THEN ta.avg_efficiency - ta.prev_week_efficiency
            ELSE NULL
        END AS efficiency_wow_change,
        CASE
            WHEN ta.prev_week_quality IS NOT NULL
            THEN ta.avg_quality_score - ta.prev_week_quality
            ELSE NULL
        END AS quality_wow_change,
        -- Moving averages
        ta.ma_7d_execution_bps,
        ta.ma_7d_efficiency,
        ta.ma_7d_quality,
        -- Performance flags
        CASE
            WHEN ta.avg_efficiency < 80 THEN 'Low Efficiency'
            WHEN ta.avg_efficiency >= 95 THEN 'High Efficiency'
            ELSE 'Normal'
        END AS efficiency_flag,
        CASE
            WHEN ta.avg_quality_score < 90 THEN 'Quality Alert'
            WHEN ta.avg_quality_score >= 98 THEN 'Excellent Quality'
            ELSE 'Normal'
        END AS quality_flag
    FROM trend_analysis ta
)

SELECT
    fm.*,
    -- Add facility attributes
    f.facility_type,
    f.operations_region,
    f.geographic_region
FROM final_metrics fm
LEFT JOIN reference_db.dim_facilities f
    ON fm.facility_id = f.facility_code
WHERE fm.date > '2024-12-26'
ORDER BY fm.facility_id, fm.date;
