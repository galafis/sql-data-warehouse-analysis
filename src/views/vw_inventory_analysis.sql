-- Inventory Analysis View
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: View for analyzing inventory levels and stock performance

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO operations_mart, public;

CREATE OR REPLACE VIEW vw_inventory_analysis AS
SELECT
    d.date_key,
    d.date_value,
    d.day_of_month,
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
    fi.quantity_on_hand,
    fi.quantity_on_order,
    fi.quantity_in_transit,
    fi.quantity_reserved,
    fi.quantity_available,
    fi.reorder_point,
    fi.reorder_quantity,
    fi.unit_cost,
    fi.total_value,
    fi.days_of_supply,
    fi.is_stock_out,
    CASE 
        WHEN fi.quantity_on_hand = 0 THEN 'Out of Stock'
        WHEN fi.quantity_on_hand <= fi.reorder_point THEN 'Low Stock'
        WHEN fi.quantity_on_hand > fi.reorder_point * 3 THEN 'Overstock'
        ELSE 'Optimal'
    END AS stock_status,
    fi.quantity_on_hand / NULLIF(fi.reorder_point, 0) AS stock_to_reorder_ratio,
    CASE 
        WHEN fi.days_of_supply < 7 THEN 'Critical'
        WHEN fi.days_of_supply < 14 THEN 'Low'
        WHEN fi.days_of_supply < 30 THEN 'Medium'
        WHEN fi.days_of_supply < 60 THEN 'Good'
        ELSE 'Excellent'
    END AS supply_status
FROM 
    integration.fact_inventory fi
JOIN 
    integration.dim_date d ON fi.date_key = d.date_key
JOIN 
    integration.dim_product p ON fi.product_key = p.product_key
JOIN 
    integration.dim_store s ON fi.store_key = s.store_key;

