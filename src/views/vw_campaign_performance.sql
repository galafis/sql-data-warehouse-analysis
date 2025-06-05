-- Campaign Performance View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for analyzing marketing campaign performance

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO marketing_mart, public;

CREATE OR REPLACE VIEW vw_campaign_performance AS
SELECT
    d.date_key,
    d.date_value,
    d.day_of_month,
    d.month_number,
    d.month_name,
    d.quarter_number,
    d.quarter_name,
    d.year_number,
    c.campaign_key,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    c.start_date,
    c.end_date,
    c.budget,
    c.target_audience,
    c.goal,
    c.kpi,
    fcp.impressions,
    fcp.clicks,
    fcp.unique_visitors,
    fcp.page_views,
    fcp.bounce_rate,
    fcp.conversion_rate,
    fcp.conversions,
    fcp.cost,
    fcp.revenue,
    fcp.roi,
    fcp.ctr,
    fcp.cost_per_click,
    fcp.cost_per_acquisition,
    fcp.revenue - fcp.cost AS profit,
    (fcp.revenue - fcp.cost) / NULLIF(fcp.cost, 0) * 100 AS roi_percent,
    fcp.cost / NULLIF(c.budget, 0) * 100 AS budget_utilization,
    CASE 
        WHEN fcp.roi < 0 THEN 'Loss'
        WHEN fcp.roi < 1 THEN 'Break Even'
        WHEN fcp.roi < 2 THEN 'Good'
        WHEN fcp.roi < 3 THEN 'Very Good'
        ELSE 'Excellent'
    END AS roi_category,
    CASE 
        WHEN fcp.conversion_rate < 0.01 THEN 'Poor'
        WHEN fcp.conversion_rate < 0.02 THEN 'Below Average'
        WHEN fcp.conversion_rate < 0.03 THEN 'Average'
        WHEN fcp.conversion_rate < 0.05 THEN 'Good'
        ELSE 'Excellent'
    END AS conversion_category
FROM 
    integration.fact_campaign_performance fcp
JOIN 
    integration.dim_date d ON fcp.date_key = d.date_key
JOIN 
    integration.dim_campaign c ON fcp.campaign_key = c.campaign_key;

