-- Load Sales Order Line Fact Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Loads the sales order line fact table from staging

DELIMITER //

CREATE OR REPLACE PROCEDURE integration.sp_load_fact_sales_order_line(
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_records_read INT DEFAULT 0;
    DECLARE v_records_inserted INT DEFAULT 0;
    DECLARE v_records_rejected INT DEFAULT 0;
    DECLARE v_error_message TEXT;
    
    -- Set the start time
    SET v_start_time = NOW();
    
    -- Log start of procedure
    INSERT INTO metadata.etl_log (
        batch_id, source_table, target_table, records_read, records_inserted, 
        records_rejected, start_time, end_time, status, message
    )
    VALUES (
        p_batch_id, 'staging.stg_order_line', 'integration.fact_sales_order_line', 0, 0, 0, 
        v_start_time, NULL, 'RUNNING', 'Starting sales order line fact load'
    );
    
    -- Begin transaction
    START TRANSACTION;
    
    -- Insert new records into fact_sales_order_line
    INSERT INTO integration.fact_sales_order_line (
        sales_order_key,
        product_key,
        promotion_key,
        order_date_key,
        source_system,
        source_order_id,
        source_order_line_id,
        quantity,
        unit_price,
        unit_cost,
        discount_percent,
        discount_amount,
        tax_amount,
        line_total,
        batch_id,
        created_at
    )
    SELECT 
        -- sales_order_key
        COALESCE(so.sales_order_key, -1) AS sales_order_key,
        -- product_key
        COALESCE(p.product_key, -1) AS product_key,
        -- promotion_key
        COALESCE(pr.promotion_key, -1) AS promotion_key,
        -- order_date_key
        COALESCE(so.order_date_key, YEAR(o.order_date) * 10000 + MONTH(o.order_date) * 100 + DAY(o.order_date)) AS order_date_key,
        -- source_system
        ol.source_system,
        -- source_order_id
        ol.source_order_id,
        -- source_order_line_id
        ol.source_order_line_id,
        -- quantity
        ol.quantity,
        -- unit_price
        ol.unit_price,
        -- unit_cost
        ol.unit_cost,
        -- discount_percent
        ol.discount_percent,
        -- discount_amount
        ol.discount_amount,
        -- tax_amount
        ol.tax_amount,
        -- line_total
        ol.line_total,
        -- batch_id
        ol.batch_id,
        -- created_at
        NOW()
    FROM 
        staging.stg_order_line ol
    JOIN 
        staging.stg_order o
    ON 
        ol.source_order_id = o.source_order_id
        AND ol.source_system = o.source_system
        AND ol.batch_id = o.batch_id
    LEFT JOIN 
        integration.fact_sales_order so
    ON 
        ol.source_order_id = so.source_order_id
        AND ol.source_system = so.source_system
    LEFT JOIN 
        integration.dim_product p
    ON 
        ol.source_product_id = p.source_product_id
        AND ol.source_system = p.source_system
        AND p.is_current = TRUE
    LEFT JOIN 
        integration.dim_promotion pr
    ON 
        ol.source_promotion_id = pr.source_promotion_id
        AND ol.source_system = pr.source_system
        AND pr.is_current = TRUE
    LEFT JOIN 
        integration.fact_sales_order_line f
    ON 
        ol.source_order_id = f.source_order_id
        AND ol.source_order_line_id = f.source_order_line_id
        AND ol.source_system = f.source_system
    WHERE 
        ol.batch_id = p_batch_id
        AND f.source_order_line_id IS NULL;
    
    -- Get the count of inserted records
    SET v_records_inserted = ROW_COUNT();
    
    -- Get the count of read records
    SELECT COUNT(*) INTO v_records_read
    FROM staging.stg_order_line
    WHERE batch_id = p_batch_id;
    
    -- Calculate rejected records
    SET v_records_rejected = v_records_read - v_records_inserted;
    
    -- Commit transaction
    COMMIT;
    
    -- Log completion of procedure
    UPDATE metadata.etl_log
    SET 
        end_time = NOW(),
        records_read = v_records_read,
        records_inserted = v_records_inserted,
        records_rejected = v_records_rejected,
        status = 'COMPLETED',
        message = CONCAT('Successfully loaded sales order line fact. Inserted: ', v_records_inserted, ', Rejected: ', v_records_rejected)
    WHERE 
        batch_id = p_batch_id AND
        source_table = 'staging.stg_order_line' AND
        target_table = 'integration.fact_sales_order_line' AND
        status = 'RUNNING';
    
    -- Return success message
    SELECT CONCAT('Sales order line fact loaded successfully. Inserted: ', v_records_inserted, ', Rejected: ', v_records_rejected) AS result;
    
    -- Exception handling
    EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction
        ROLLBACK;
        
        -- Get error message
        SET v_error_message = CONCAT('Error loading sales order line fact: ', SQLERRM);
        
        -- Log error
        UPDATE metadata.etl_log
        SET 
            end_time = NOW(),
            records_read = v_records_read,
            records_inserted = v_records_inserted,
            records_rejected = v_records_rejected,
            status = 'FAILED',
            message = v_error_message
        WHERE 
            batch_id = p_batch_id AND
            source_table = 'staging.stg_order_line' AND
            target_table = 'integration.fact_sales_order_line' AND
            status = 'RUNNING';
        
        -- Insert error log
        INSERT INTO metadata.etl_error_log (
            batch_id, source_table, target_table, error_message, record_data, created_at
        )
        VALUES (
            p_batch_id, 'staging.stg_order_line', 'integration.fact_sales_order_line', v_error_message, NULL, NOW()
        );
        
        -- Re-raise the exception
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
END //

DELIMITER ;

