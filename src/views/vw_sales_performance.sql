-- Sales Performance View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for analyzing sales performance across various dimensions

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO sales_mart, public;

CREATE OR REPLACE VIEW vw_sales_performance AS
SELECT
    d.date_key,
    d.date_value,
    d.day_of_month,
    d.day_name,
    d.month_number,
    d.month_name,
    d.quarter_number,
    d.quarter_name,
    d.year_number,
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    s.store_key,
    s.store_name,
    s.store_type,
    s.city,
    s.state_province,
    s.country,
    s.region,
    c.customer_key,
    c.customer_name,
    c.customer_type,
    c.city AS customer_city,
    c.state_province AS customer_state,
    c.country AS customer_country,
    c.region AS customer_region,
    SUM(fsol.quantity) AS total_quantity,
    SUM(fsol.line_total_amount) AS total_sales,
    SUM(fsol.discount_amount) AS total_discount,
    SUM(fsol.tax_amount) AS total_tax,
    SUM(fsol.gross_profit) AS total_profit,
    SUM(fsol.gross_profit) / NULLIF(SUM(fsol.line_total_amount), 0) * 100 AS profit_margin,
    COUNT(DISTINCT fso.sales_order_key) AS order_count,
    SUM(fsol.line_total_amount) / NULLIF(COUNT(DISTINCT fso.sales_order_key), 0) AS avg_order_value,
    SUM(fsol.quantity) / NULLIF(COUNT(DISTINCT fsol.sales_order_line_key), 0) AS avg_quantity_per_line
FROM 
    integration.fact_sales_order_line fsol
JOIN 
    integration.fact_sales_order fso ON fsol.sales_order_key = fso.sales_order_key
JOIN 
    integration.dim_date d ON fso.order_date_key = d.date_key
JOIN 
    integration.dim_product p ON fsol.product_key = p.product_key
JOIN 
    integration.dim_store s ON fso.store_key = s.store_key
JOIN 
    integration.dim_customer c ON fso.customer_key = c.customer_key
WHERE
    fso.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY
    d.date_key,
    d.date_value,
    d.day_of_month,
    d.day_name,
    d.month_number,
    d.month_name,
    d.quarter_number,
    d.quarter_name,
    d.year_number,
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    s.store_key,
    s.store_name,
    s.store_type,
    s.city,
    s.state_province,
    s.country,
    s.region,
    c.customer_key,
    c.customer_name,
    c.customer_type,
    c.city,
    c.state_province,
    c.country,
    c.region;

