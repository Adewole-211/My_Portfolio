-- Order Cancellation Analysis Query Template
-- Purpose: Analyze order cancellations by reason and calculate DPMO metrics
-- Usage: Replace placeholder names with your actual database/table/column names

WITH cancellation_data AS (
    SELECT
        facility_id,
        DATE(scheduled_delivery_date) AS ship_date,

        -- Execution-related cancellations
        SUM(CASE WHEN cancel_reason IN (
            'PROCESSING_FAILURE',
            'LATE_SHIPMENT',
            'LOST_INVENTORY',
            'INSUFFICIENT_STOCK',
            'MISSED_DEADLINE',
            'SHIPPING_ERROR',
            'QUALITY_ISSUE',
            'PACKAGING_PROBLEM'
        ) THEN quantity_cancelled ELSE 0 END) AS execution_cancellation,

        -- Manual/Administrative cancellations
        SUM(CASE WHEN cancel_reason IN (
            'CUSTOMER_REQUESTED',
            'FORCE_CANCEL',
            'ADMIN_REQUESTED',
            'MANUAL_OVERRIDE'
        ) THEN quantity_cancelled ELSE 0 END) AS manual_cancellation,

        -- Planning-related cancellations
        SUM(CASE WHEN cancel_reason IN (
            'CAPACITY_CONSTRAINT',
            'DELAYED_SUPPLY',
            'DEPENDENCY_MISSING',
            'FACILITY_UNAVAILABLE',
            'SYSTEM_MAINTENANCE',
            'INVENTORY_UNAVAILABLE',
            'ITEM_DISCONTINUED',
            'SUPPLIER_ISSUE',
            'LOCATION_RESTRICTED',
            'SUPPLY_CHAIN_DISRUPTION',
            'REPLENISHMENT_DELAY',
            'RESTRICTED_LOCATION'
        ) THEN quantity_cancelled ELSE 0 END) AS planning_cancellation,

        -- System/Technical cancellations
        SUM(CASE WHEN cancel_reason IN (
            'INVALID_DATE',
            'INVALID_SCHEDULE',
            'INVALID_REQUEST',
            'VALIDATION_FAILED',
            'DATA_MISMATCH',
            'SYSTEM_ERROR',
            'TECHNICAL_FAILURE',
            'SOFTWARE_ISSUE',
            'UNABLE_TO_PROCESS',
            'CONFIGURATION_ERROR'
        ) THEN quantity_cancelled ELSE 0 END) AS system_cancellation,

        SUM(quantity_requested) AS total_quantity_requested

    FROM orders_db.fact_order_items
    WHERE scheduled_delivery_date > '2024-12-26 00:00:00'
    GROUP BY 1, 2
),

dpmo_calculations AS (
    SELECT
        t.*,
        f.country_code,
        f.operations_group,
        f.site_type,
        f.region_name,
        -- Calculate DPMO (Defects Per Million Opportunities) for each category
        (t.execution_cancellation::DECIMAL / NULLIF(t.total_quantity_requested, 0)) * 1000000 AS execution_dpmo,
        (t.manual_cancellation::DECIMAL / NULLIF(t.total_quantity_requested, 0)) * 1000000 AS manual_dpmo,
        (t.planning_cancellation::DECIMAL / NULLIF(t.total_quantity_requested, 0)) * 1000000 AS planning_dpmo,
        (t.system_cancellation::DECIMAL / NULLIF(t.total_quantity_requested, 0)) * 1000000 AS system_dpmo,
        -- Calculate total cancellation rate
        ((t.execution_cancellation + t.manual_cancellation + t.planning_cancellation + t.system_cancellation)::DECIMAL /
         NULLIF(t.total_quantity_requested, 0)) * 100 AS total_cancellation_rate_percent
    FROM cancellation_data t
    LEFT JOIN reference_db.dim_facilities f
        ON t.facility_id = f.facility_id
    WHERE t.ship_date > '2024-12-26'
        AND f.country_code IS NOT NULL
),

weekly_summary AS (
    SELECT
        facility_id,
        country_code,
        operations_group,
        site_type,
        region_name,
        DATE_TRUNC('week', ship_date) AS week_start_date,
        EXTRACT(week FROM ship_date) AS week_number,
        EXTRACT(year FROM ship_date) AS year,
        -- Aggregate weekly metrics
        SUM(execution_cancellation) AS weekly_execution_cancellations,
        SUM(manual_cancellation) AS weekly_manual_cancellations,
        SUM(planning_cancellation) AS weekly_planning_cancellations,
        SUM(system_cancellation) AS weekly_system_cancellations,
        SUM(total_quantity_requested) AS weekly_total_requests,
        AVG(execution_dpmo) AS avg_execution_dpmo,
        AVG(manual_dpmo) AS avg_manual_dpmo,
        AVG(planning_dpmo) AS avg_planning_dpmo,
        AVG(system_dpmo) AS avg_system_dpmo,
        AVG(total_cancellation_rate_percent) AS avg_cancellation_rate
    FROM dpmo_calculations
    GROUP BY 1,2,3,4,5,6,7,8
)

-- Final output with both daily and weekly aggregations
SELECT
    'daily' AS aggregation_level,
    facility_id,
    ship_date AS date,
    NULL AS week_start_date,
    NULL AS week_number,
    EXTRACT(year FROM ship_date) AS year,
    country_code,
    operations_group,
    site_type,
    region_name,
    execution_cancellation AS execution_count,
    manual_cancellation AS manual_count,
    planning_cancellation AS planning_count,
    system_cancellation AS system_count,
    total_quantity_requested,
    execution_dpmo,
    manual_dpmo,
    planning_dpmo,
    system_dpmo,
    total_cancellation_rate_percent AS cancellation_rate
FROM dpmo_calculations

UNION ALL

SELECT
    'weekly' AS aggregation_level,
    facility_id,
    NULL AS date,
    week_start_date,
    week_number,
    year,
    country_code,
    operations_group,
    site_type,
    region_name,
    weekly_execution_cancellations AS execution_count,
    weekly_manual_cancellations AS manual_count,
    weekly_planning_cancellations AS planning_count,
    weekly_system_cancellations AS system_count,
    weekly_total_requests AS total_quantity_requested,
    avg_execution_dpmo AS execution_dpmo,
    avg_manual_dpmo AS manual_dpmo,
    avg_planning_dpmo AS planning_dpmo,
    avg_system_dpmo AS system_dpmo,
    avg_cancellation_rate AS cancellation_rate
FROM weekly_summary

ORDER BY facility_id, aggregation_level DESC, COALESCE(date, week_start_date);
