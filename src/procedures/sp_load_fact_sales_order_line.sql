-- Load Sales Order Line Fact Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Stored procedure to load the sales order line fact table from staging

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

DELIMITER //

CREATE OR REPLACE PROCEDURE sp_load_fact_sales_order_line(
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_source_system VARCHAR(50);
    DECLARE v_records_inserted INT DEFAULT 0;
    DECLARE v_records_rejected INT DEFAULT 0;
    
    -- Set source system
    SET v_source_system = 'ERP_SYSTEM';
    
    -- Start transaction
    START TRANSACTION;
    
    -- Insert records from staging to fact table
    INSERT INTO fact_sales_order_line (
        sales_order_key,
        product_key,
        promotion_key,
        business_key,
        source_system,
        source_order_id,
        source_order_line_id,
        quantity,
        unit_price,
        unit_cost,
        discount_percent,
        discount_amount,
        tax_amount,
        line_total_amount,
        gross_profit
    )
    SELECT 
        fso.sales_order_key,
        dp.product_key,
        dpr.promotion_key,
        CONCAT(ol.source_system, ':', ol.source_order_id, ':', ol.source_order_line_id),
        ol.source_system,
        ol.source_order_id,
        ol.source_order_line_id,
        ol.quantity,
        ol.unit_price,
        ol.unit_cost,
        ol.discount_percent,
        ol.discount_amount,
        ol.tax_amount,
        ol.line_total,
        (ol.line_total - (ol.quantity * ol.unit_cost)) AS gross_profit
    FROM 
        staging.stg_order_line ol
    JOIN 
        fact_sales_order fso ON CONCAT(ol.source_system, ':', ol.source_order_id) = fso.business_key
    JOIN 
        dim_product dp ON CONCAT(ol.source_system, ':', ol.source_product_id) = dp.business_key AND dp.is_current = TRUE
    LEFT JOIN 
        dim_promotion dpr ON CONCAT(ol.source_system, ':', ol.source_promotion_id) = dpr.business_key AND dpr.is_current = TRUE
    WHERE 
        ol.batch_id = p_batch_id
        AND NOT EXISTS (
            SELECT 1 
            FROM fact_sales_order_line fsol 
            WHERE fsol.business_key = CONCAT(ol.source_system, ':', ol.source_order_id, ':', ol.source_order_line_id)
        );
    
    -- Get the number of records inserted
    SET v_records_inserted = ROW_COUNT();
    
    -- Insert rejected records into error log table
    INSERT INTO metadata.etl_error_log (
        batch_id,
        source_table,
        target_table,
        error_message,
        record_data,
        created_at
    )
    SELECT 
        p_batch_id,
        'stg_order_line',
        'fact_sales_order_line',
        CONCAT('Missing dimension key for: ', 
               CASE 
                   WHEN fso.sales_order_key IS NULL THEN 'sales_order_key' 
                   WHEN dp.product_key IS NULL THEN 'product_key'
                   ELSE 'unknown'
               END),
        CONCAT('source_order_id: ', ol.source_order_id, ', source_order_line_id: ', ol.source_order_line_id),
        NOW()
    FROM 
        staging.stg_order_line ol
    LEFT JOIN 
        fact_sales_order fso ON CONCAT(ol.source_system, ':', ol.source_order_id) = fso.business_key
    LEFT JOIN 
        dim_product dp ON CONCAT(ol.source_system, ':', ol.source_product_id) = dp.business_key AND dp.is_current = TRUE
    WHERE 
        ol.batch_id = p_batch_id
        AND NOT EXISTS (
            SELECT 1 
            FROM fact_sales_order_line fsol 
            WHERE fsol.business_key = CONCAT(ol.source_system, ':', ol.source_order_id, ':', ol.source_order_line_id)
        )
        AND (fso.sales_order_key IS NULL OR dp.product_key IS NULL);
    
    -- Get the number of records rejected
    SET v_records_rejected = ROW_COUNT();
    
    -- Update the ETL log
    INSERT INTO metadata.etl_log (
        batch_id,
        source_table,
        target_table,
        records_read,
        records_inserted,
        records_rejected,
        start_time,
        end_time,
        status,
        message
    )
    VALUES (
        p_batch_id,
        'stg_order_line',
        'fact_sales_order_line',
        v_records_inserted + v_records_rejected,
        v_records_inserted,
        v_records_rejected,
        NOW(),
        NOW(),
        'COMPLETED',
        CONCAT('Successfully loaded ', v_records_inserted, ' records into fact_sales_order_line. ', 
               v_records_rejected, ' records were rejected.')
    );
    
    -- Commit transaction
    COMMIT;
    
    -- Return the results
    SELECT 
        p_batch_id AS batch_id,
        v_records_inserted AS records_inserted,
        v_records_rejected AS records_rejected,
        'COMPLETED' AS status;
END //

DELIMITER ;

