-- Inventory Analysis View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for inventory analysis

CREATE OR REPLACE VIEW operations_mart.vw_inventory_analysis AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    p.category,
    p.subcategory,
    p.brand,
    p.product_name,
    s.store_name,
    s.city AS store_city,
    s.state_province AS store_state,
    s.country AS store_country,
    s.region AS store_region,
    AVG(i.quantity_on_hand) AS avg_quantity_on_hand,
    AVG(i.quantity_available) AS avg_quantity_available,
    AVG(i.quantity_on_order) AS avg_quantity_on_order,
    AVG(i.quantity_in_transit) AS avg_quantity_in_transit,
    AVG(i.quantity_reserved) AS avg_quantity_reserved,
    AVG(i.total_value) AS avg_inventory_value,
    AVG(i.days_of_supply) AS avg_days_of_supply,
    SUM(CASE WHEN i.is_stock_out = TRUE THEN 1 ELSE 0 END) AS stock_out_count,
    COUNT(*) AS inventory_count,
    SUM(CASE WHEN i.is_stock_out = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS stock_out_rate,
    AVG(i.quantity_on_hand * p.unit_cost) AS avg_inventory_cost,
    AVG(CASE WHEN i.quantity_on_hand > i.reorder_point THEN 0 ELSE 1 END) * 100 AS reorder_needed_rate
FROM
    integration.fact_inventory i
JOIN
    integration.dim_date d ON i.date_key = d.date_key
JOIN
    integration.dim_product p ON i.product_key = p.product_key
JOIN
    integration.dim_store s ON i.store_key = s.store_key
GROUP BY
    d.year_number,
    d.month_number,
    d.month_name,
    d.quarter,
    d.quarter_name,
    p.category,
    p.subcategory,
    p.brand,
    p.product_name,
    s.store_name,
    s.city,
    s.state_province,
    s.country,
    s.region;

