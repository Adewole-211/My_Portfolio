-- Multi-Metric Performance Dashboard Query Template
-- Purpose: Comprehensive performance tracking across multiple facilities and metrics
-- Usage: Replace placeholder names with your actual database/table/column names

WITH daily_metrics AS (
    SELECT
        region_code,
        DATE_TRUNC('week', DATE(metric_date) + INTERVAL '1 day') - INTERVAL '1 day' AS week_start_date,
        SUM(total_volume) as total_volume
    FROM warehouse_db.metrics_schema.fact_daily_volume_by_region
    WHERE metric_date > '2024-12-31 00:00:00'
    GROUP BY 1, 2
),

performance_metrics AS (
    SELECT
        t.facility_id,
        DATE(t.metric_date) AS date,
        f.region_name,
        MAX(d.total_volume) as total_volume,
        SUM(t.total_impact) AS total_impact,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.labor_impact, 0) + COALESCE(t.capacity_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS execution_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.event_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS event_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.capacity_constraint_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS capacity_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.unallocated_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS unallocated_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.complexity_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS complexity_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.training_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS training_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.downtime_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS downtime_bps,
        CASE
            WHEN MAX(d.total_volume) > 0
            THEN (SUM(COALESCE(t.planning_impact, 0)) / MAX(d.total_volume)) * 10000
            ELSE 0
        END AS planning_bps
    FROM performance_schema.fact_daily_impact t
    LEFT JOIN daily_metrics d
        ON t.region_code = d.region_code
        AND DATE_TRUNC('week', DATE(t.metric_date) + INTERVAL '1 day') - INTERVAL '1 day' = d.week_start_date
    LEFT JOIN reference_db.dim_facility f
        ON t.facility_id = f.facility_code
    WHERE t.total_impact > 0 AND t.metric_type = 'DAILY'
    GROUP BY 1, 2, 3
    ORDER BY 2
),

quality_metrics AS (
    SELECT
        facility_id AS facility_code,
        EXTRACT(year FROM process_date + INTERVAL '1 day') AS year,
        (process_date + INTERVAL '1 day')::DATE AS date_full,
        SUM(defect_units) AS quality_miss_units
    FROM quality_db.fact_quality_metrics
    WHERE process_date >= current_timestamp - interval '14 month'
    GROUP BY facility_id, year, date_full
),

operational_hours AS (
    SELECT
        facility_code,
        process_date as date,
        region_name,
        SUM(overstaffing_hours_not_actioned) AS os_hrs_not_actioned,
        SUM(overstaffing_hours_unplanned) AS os_hrs_unplanned,
        SUM(overstaffing_hours_not_adopted) AS os_hrs_not_adopted,
        SUM(understaffing_hours_not_actioned) AS us_hrs_not_actioned,
        SUM(understaffing_hours_unplanned) AS us_hrs_unplanned,
        SUM(understaffing_hours_not_adopted) AS us_hrs_not_adopted,
        SUM(overstaffing_cost_not_actioned) AS os_cost_not_actioned,
        SUM(overstaffing_cost_unplanned) AS os_cost_unplanned,
        SUM(overstaffing_cost_not_adopted) AS os_cost_not_adopted,
        SUM(understaffing_cost_not_actioned) AS us_cost_not_actioned,
        SUM(understaffing_cost_unplanned) AS us_cost_unplanned,
        SUM(understaffing_cost_not_adopted) AS us_cost_not_adopted
    FROM operations_db.fact_staffing_defects
    WHERE process_date > '2024-12-31 00:00:00'
    GROUP BY 1, 2, 3
),

metric_config AS (
    SELECT 'Output' AS pillar, 'Speed' AS section, 'Performance Impact' AS metric_group, 'Execution' AS metric_name, 'bps' AS unit
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Training', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Planning', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Capacity Constraint', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Complexity', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Event', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Unallocated', 'bps'
    UNION ALL SELECT 'Output', 'Speed', 'Performance Impact', 'Downtime', 'bps'
    UNION ALL SELECT 'Output', 'Quality', 'Quality Metrics', 'Defect Units', 'units'
    UNION ALL SELECT 'Output', 'Cost', 'Staffing Hours', 'Overstaffing Hours Not Actioned', 'hours'
    UNION ALL SELECT 'Output', 'Cost', 'Staffing Hours', 'Overstaffing Hours Unplanned', 'hours'
    UNION ALL SELECT 'Output', 'Cost', 'Staffing Cost', 'Overstaffing Cost Not Actioned', 'cost'
    -- Add more metric configurations as needed
),

base_dataset AS (
    SELECT
        COALESCE(pm.facility_id, oh.facility_code, qm.facility_code) as facility_id,
        COALESCE(pm.date, oh.date, qm.date_full) as date,
        DATEADD(day, -CAST(DATE_PART(dow, COALESCE(pm.date, oh.date, qm.date_full)) AS INTEGER),
               COALESCE(pm.date, oh.date, qm.date_full))::date AS start_of_week,
        DATE_PART(week, COALESCE(pm.date, oh.date, qm.date_full) + 1)::int AS week_number,
        EXTRACT(YEAR FROM COALESCE(pm.date, oh.date, qm.date_full))::int AS year,
        COALESCE(pm.region_name, oh.region_name) as region_name,
        -- Performance metrics
        pm.execution_bps,
        pm.training_bps,
        pm.downtime_bps,
        pm.event_bps,
        pm.capacity_bps,
        pm.unallocated_bps,
        pm.complexity_bps,
        pm.planning_bps,
        -- Quality metrics
        qm.quality_miss_units,
        -- Operational metrics
        oh.os_hrs_not_actioned,
        oh.os_hrs_unplanned,
        oh.os_hrs_not_adopted,
        oh.us_hrs_not_actioned,
        oh.us_hrs_unplanned,
        oh.us_hrs_not_adopted,
        oh.os_cost_not_actioned,
        oh.os_cost_unplanned,
        oh.os_cost_not_adopted,
        oh.us_cost_not_actioned,
        oh.us_cost_unplanned,
        oh.us_cost_not_adopted
    FROM performance_metrics pm
    FULL OUTER JOIN operational_hours oh
        ON pm.facility_id = oh.facility_code AND pm.date = oh.date
    FULL OUTER JOIN quality_metrics qm
        ON COALESCE(pm.facility_id, oh.facility_code) = qm.facility_code
        AND COALESCE(pm.date, oh.date) = qm.date_full
)

SELECT
    bd.facility_id,
    bd.date,
    bd.week_number,
    bd.year,
    bd.region_name,
    mc.pillar,
    mc.section,
    mc.metric_group,
    mc.metric_name,
    mc.unit,
    CASE
        WHEN mc.metric_name = 'Execution' AND mc.unit = 'bps' THEN bd.execution_bps
        WHEN mc.metric_name = 'Training' THEN bd.training_bps
        WHEN mc.metric_name = 'Planning' THEN bd.planning_bps
        WHEN mc.metric_name = 'Capacity Constraint' THEN bd.capacity_bps
        WHEN mc.metric_name = 'Complexity' THEN bd.complexity_bps
        WHEN mc.metric_name = 'Event' THEN bd.event_bps
        WHEN mc.metric_name = 'Unallocated' THEN bd.unallocated_bps
        WHEN mc.metric_name = 'Downtime' THEN bd.downtime_bps
        WHEN mc.metric_name = 'Defect Units' THEN bd.quality_miss_units
        WHEN mc.metric_name = 'Overstaffing Hours Not Actioned' THEN bd.os_hrs_not_actioned
        WHEN mc.metric_name = 'Overstaffing Hours Unplanned' THEN bd.os_hrs_unplanned
        WHEN mc.metric_name = 'Overstaffing Cost Not Actioned' THEN bd.os_cost_not_actioned
        -- Add more metric mappings as needed
    END AS metric_value
FROM base_dataset bd
CROSS JOIN metric_config mc
WHERE bd.date > '2024-12-26';
