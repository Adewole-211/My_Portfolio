-- Operational Metrics and Facility Performance Query Template
-- Purpose: Track operational performance across facilities
-- Usage: Replace placeholder names with your actual database/table/column names

WITH facility_list AS (
    SELECT
        daf.facility_code,
        df.operations_region,
        daf.facility_type,
        daf.geographic_region,
        daf.country_name,
        daf.hub_name
    FROM reference_db.dim_all_facilities daf
    LEFT JOIN reference_db.dim_facilities df
        ON daf.facility_code = df.facility_code
    WHERE daf.hub_name IS NOT NULL
    GROUP BY 1,2,3,4,5,6
    ORDER BY facility_code ASC
)

SELECT
    facility_code,
    process_date,
    country_name,
    facility_type,
    geographic_region,
    operations_region,
    hub_name,
    TO_CHAR(process_date, 'IW') AS week_number,
    TO_CHAR(process_date, 'IYYY') AS year,
    -- Operational Hours Metrics
    SUM(overstaffing_hours_not_actioned) AS os_hrs_not_actioned,
    SUM(overstaffing_hours_unplanned) AS os_hrs_unplanned,
    SUM(overstaffing_hours_not_adopted) AS os_hrs_not_adopted,
    SUM(understaffing_hours_not_actioned) AS us_hrs_not_actioned,
    SUM(understaffing_hours_unplanned) AS us_hrs_unplanned,
    SUM(understaffing_hours_not_adopted) AS us_hrs_not_adopted,
    -- Cost Metrics
    SUM(overstaffing_cost_not_actioned) AS os_cost_not_actioned,
    SUM(overstaffing_cost_unplanned) AS os_cost_unplanned,
    SUM(overstaffing_cost_not_adopted) AS os_cost_not_adopted,
    SUM(understaffing_cost_not_actioned) AS us_cost_not_actioned,
    SUM(understaffing_cost_unplanned) AS us_cost_unplanned,
    SUM(understaffing_cost_not_adopted) AS us_cost_not_adopted,
    -- Performance Metrics
    AVG(efficiency_percentage) AS avg_efficiency,
    SUM(total_processed_volume) AS total_volume,
    SUM(error_count) AS total_errors,
    AVG(quality_score) AS avg_quality_score
FROM operations_db.fact_operational_metrics
JOIN facility_list USING (facility_code)
WHERE process_date > '2024-12-31 00:00:00'
    AND operations_region IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY facility_code, process_date;