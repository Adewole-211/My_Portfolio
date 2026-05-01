-- Cost Analysis and Budget Variance Query Template
-- Purpose: Analyze cost performance and budget variances across facilities
-- Usage: Replace placeholder names with your actual database/table/column names

WITH base_data AS (
    SELECT *
    FROM cost_db.fact_cost_analysis
),

calculations AS (
    SELECT
        facility_code,
        week_number,
        year,
        region_code,
        cost_category,
        SUM(CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END) AS total_over_budget_cost,
        SUM(planned_cost) as total_planned_cost
    FROM base_data
    GROUP BY 1,2,3,4,5
),

weekly_summary AS (
    SELECT
        facility_code,
        week_number,
        year,
        SUM(total_over_budget_cost) as affected_amount,
        CASE
            WHEN SUM(total_planned_cost) > 0
            THEN SUM(total_over_budget_cost::real) / SUM(total_planned_cost::real)
            ELSE 0
        END AS weekly_variance_ratio
    FROM calculations
    GROUP BY 1,2,3
),

complete_analysis as (
    SELECT
        b.*,
        w.affected_amount,
        w.weekly_variance_ratio,
        CASE
            WHEN over_budget > 0 AND w.affected_amount > 0
            THEN b.planned_cost / w.affected_amount
            ELSE 0
        END AS cost_share_ratio,
        CASE
            WHEN over_budget > 0 AND w.affected_amount > 0
            THEN (w.weekly_variance_ratio * (b.planned_cost / w.affected_amount)) * 10000
            ELSE 0
        END AS variance_basis_points
    FROM base_data b
    LEFT JOIN weekly_summary w
        ON b.facility_code = w.facility_code
        AND b.year = w.year
        AND b.week_number = w.week_number
),

cost_summary AS (
    SELECT DISTINCT
        facility_code,
        start_of_week as date,
        CASE
            WHEN region_code = 'AT' THEN 'Austria'
            WHEN region_code = 'BE' THEN 'Belgium'
            WHEN region_code = 'DE' THEN 'Germany'
            WHEN region_code = 'ES' THEN 'Spain'
            WHEN region_code = 'CZ' THEN 'Czech Republic'
            WHEN region_code = 'FR' THEN 'France'
            WHEN region_code = 'GB' THEN 'United Kingdom'
            WHEN region_code = 'IE' THEN 'Ireland'
            WHEN region_code = 'IT' THEN 'Italy'
            WHEN region_code = 'LU' THEN 'Luxembourg'
            WHEN region_code = 'NL' THEN 'Netherlands'
            WHEN region_code = 'PL' THEN 'Poland'
            WHEN region_code = 'PT' THEN 'Portugal'
            WHEN region_code = 'SE' THEN 'Sweden'
            WHEN region_code = 'TR' THEN 'Turkey'
            ELSE region_code
        END AS country_name,
        SUM(CASE WHEN cost_category = 'Labor_Backlog' THEN variance_basis_points ELSE 0 END) AS labor_backlog_bps,
        SUM(CASE WHEN cost_category = 'Equipment_Exception' THEN variance_basis_points ELSE 0 END) AS equipment_exception_bps,
        SUM(CASE WHEN cost_category = 'Execution_Variance' THEN variance_basis_points ELSE 0 END) AS execution_variance_bps,
        SUM(CASE WHEN cost_category = 'Planning_Variance' THEN variance_basis_points ELSE 0 END) AS planning_variance_bps,
        SUM(CASE WHEN (cost_category IS NULL OR cost_category = '') THEN variance_basis_points ELSE 0 END) AS unknown_variance_bps,
        SUM(CASE WHEN cost_category = 'Labor_Backlog' THEN CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END ELSE 0 END) AS labor_backlog_cost_affected,
        SUM(CASE WHEN cost_category = 'Equipment_Exception' THEN CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END ELSE 0 END) AS equipment_exception_cost_affected,
        SUM(CASE WHEN cost_category = 'Execution_Variance' THEN CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END ELSE 0 END) AS execution_variance_cost_affected,
        SUM(CASE WHEN cost_category = 'Planning_Variance' THEN CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END ELSE 0 END) AS planning_variance_cost_affected,
        SUM(CASE WHEN (cost_category IS NULL OR cost_category = '') THEN CASE WHEN over_budget > 0 THEN planned_cost ELSE 0 END ELSE 0 END) AS unknown_variance_cost_affected
    FROM complete_analysis
    GROUP BY 1, 2, 3
)

SELECT
    cs.*,
    f.facility_type,
    f.region_name,
    f.hub_name
FROM cost_summary cs
LEFT JOIN reference_db.dim_facilities f
    ON cs.facility_code = f.facility_code
WHERE cs.date > '2024-12-26'
ORDER BY cs.facility_code, cs.date;