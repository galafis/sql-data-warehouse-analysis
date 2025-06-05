-- Sales Performance View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for sales performance analysis

CREATE OR REPLACE VIEW sales_mart.vw_sales_performance AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    p.category,
    p.subcategory,
    p.brand,
    s.store_name,
    s.city AS store_city,
    s.state_province AS store_state,
    s.country AS store_country,
    s.region AS store_region,
    COUNT(DISTINCT sol.sales_order_key) AS order_count,
    SUM(sol.quantity) AS unit_count,
    SUM(sol.line_total) AS total_sales,
    SUM(sol.discount_amount) AS total_discount,
    SUM(sol.tax_amount) AS total_tax,
    SUM(sol.unit_cost * sol.quantity) AS total_cost,
    SUM(sol.profit_amount) AS total_profit,
    AVG(sol.profit_margin) * 100 AS avg_profit_margin_percent,
    SUM(sol.line_total) / SUM(sol.quantity) AS avg_unit_price,
    SUM(sol.line_total) / COUNT(DISTINCT sol.sales_order_key) AS avg_order_value
FROM
    integration.fact_sales_order_line sol
JOIN
    integration.fact_sales_order so ON sol.sales_order_key = so.sales_order_key
JOIN
    integration.dim_date d ON sol.order_date_key = d.date_key
JOIN
    integration.dim_product p ON sol.product_key = p.product_key
JOIN
    integration.dim_store s ON so.store_key = s.store_key
WHERE
    so.order_status = 'COMPLETED'
GROUP BY
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    p.category,
    p.subcategory,
    p.brand,
    s.store_name,
    s.city,
    s.state_province,
    s.country,
    s.region;

