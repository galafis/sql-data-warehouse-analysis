-- Campaign Performance View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for marketing campaign performance analysis

CREATE OR REPLACE VIEW marketing_mart.vw_campaign_performance AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    c.target_audience,
    c.goal,
    c.kpi,
    SUM(cp.impressions) AS total_impressions,
    SUM(cp.clicks) AS total_clicks,
    SUM(cp.unique_visitors) AS total_unique_visitors,
    SUM(cp.page_views) AS total_page_views,
    AVG(cp.bounce_rate) AS avg_bounce_rate,
    AVG(cp.conversion_rate) AS avg_conversion_rate,
    SUM(cp.conversions) AS total_conversions,
    SUM(cp.cost) AS total_cost,
    SUM(cp.revenue) AS total_revenue,
    CASE 
        WHEN SUM(cp.cost) > 0 THEN (SUM(cp.revenue) - SUM(cp.cost)) / SUM(cp.cost) * 100 
        ELSE 0 
    END AS roi_percent,
    CASE 
        WHEN SUM(cp.impressions) > 0 THEN SUM(cp.clicks) / SUM(cp.impressions) * 100 
        ELSE 0 
    END AS ctr_percent,
    CASE 
        WHEN SUM(cp.clicks) > 0 THEN SUM(cp.cost) / SUM(cp.clicks) 
        ELSE 0 
    END AS cost_per_click,
    CASE 
        WHEN SUM(cp.conversions) > 0 THEN SUM(cp.cost) / SUM(cp.conversions) 
        ELSE 0 
    END AS cost_per_acquisition,
    CASE 
        WHEN SUM(cp.conversions) > 0 THEN SUM(cp.revenue) / SUM(cp.conversions) 
        ELSE 0 
    END AS revenue_per_conversion
FROM
    integration.fact_campaign_performance cp
JOIN
    integration.dim_date d ON cp.date_key = d.date_key
JOIN
    integration.dim_campaign c ON cp.campaign_key = c.campaign_key
GROUP BY
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    c.target_audience,
    c.goal,
    c.kpi;

