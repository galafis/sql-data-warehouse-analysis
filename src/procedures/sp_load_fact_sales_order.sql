-- Load Sales Order Fact Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Loads the sales order fact table from staging

DELIMITER //

CREATE OR REPLACE PROCEDURE integration.sp_load_fact_sales_order(
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
        p_batch_id, 'staging.stg_order', 'integration.fact_sales_order', 0, 0, 0, 
        v_start_time, NULL, 'RUNNING', 'Starting sales order fact load'
    );
    
    -- Begin transaction
    START TRANSACTION;
    
    -- Insert new records into fact_sales_order
    INSERT INTO integration.fact_sales_order (
        order_date_key,
        customer_key,
        store_key,
        employee_key,
        payment_method_key,
        shipping_method_key,
        currency_key,
        source_system,
        source_order_id,
        order_date,
        order_status,
        payment_status,
        shipping_status,
        subtotal,
        tax_amount,
        shipping_amount,
        discount_amount,
        total_amount,
        order_line_count,
        batch_id,
        created_at
    )
    SELECT 
        -- order_date_key (YYYYMMDD format)
        YEAR(o.order_date) * 10000 + MONTH(o.order_date) * 100 + DAY(o.order_date) AS order_date_key,
        -- customer_key
        COALESCE(c.customer_key, -1) AS customer_key,
        -- store_key
        COALESCE(s.store_key, -1) AS store_key,
        -- employee_key
        COALESCE(e.employee_key, -1) AS employee_key,
        -- payment_method_key
        COALESCE(pm.payment_method_key, -1) AS payment_method_key,
        -- shipping_method_key
        COALESCE(sm.shipping_method_key, -1) AS shipping_method_key,
        -- currency_key
        COALESCE(cu.currency_key, -1) AS currency_key,
        -- source_system
        o.source_system,
        -- source_order_id
        o.source_order_id,
        -- order_date
        o.order_date,
        -- order_status
        o.order_status,
        -- payment_status
        o.payment_status,
        -- shipping_status
        o.shipping_status,
        -- subtotal
        o.subtotal,
        -- tax_amount
        o.tax_amount,
        -- shipping_amount
        o.shipping_amount,
        -- discount_amount
        o.discount_amount,
        -- total_amount
        o.total_amount,
        -- order_line_count
        (SELECT COUNT(*) FROM staging.stg_order_line ol WHERE ol.source_order_id = o.source_order_id AND ol.source_system = o.source_system AND ol.batch_id = o.batch_id) AS order_line_count,
        -- batch_id
        o.batch_id,
        -- created_at
        NOW()
    FROM 
        staging.stg_order o
    LEFT JOIN 
        integration.dim_customer c
    ON 
        o.source_customer_id = c.source_customer_id
        AND o.source_system = c.source_system
        AND c.is_current = TRUE
    LEFT JOIN 
        integration.dim_store s
    ON 
        o.source_store_id = s.source_store_id
        AND o.source_system = s.source_system
        AND s.is_current = TRUE
    LEFT JOIN 
        integration.dim_employee e
    ON 
        o.source_employee_id = e.source_employee_id
        AND o.source_system = e.source_system
        AND e.is_current = TRUE
    LEFT JOIN 
        integration.dim_payment_method pm
    ON 
        o.payment_method = pm.payment_method_code
    LEFT JOIN 
        integration.dim_shipping_method sm
    ON 
        o.shipping_method = sm.shipping_method_code
    LEFT JOIN 
        integration.dim_currency cu
    ON 
        o.currency_code = cu.currency_code
    LEFT JOIN 
        integration.fact_sales_order f
    ON 
        o.source_order_id = f.source_order_id
        AND o.source_system = f.source_system
    WHERE 
        o.batch_id = p_batch_id
        AND f.source_order_id IS NULL;
    
    -- Get the count of inserted records
    SET v_records_inserted = ROW_COUNT();
    
    -- Get the count of read records
    SELECT COUNT(*) INTO v_records_read
    FROM staging.stg_order
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
        message = CONCAT('Successfully loaded sales order fact. Inserted: ', v_records_inserted, ', Rejected: ', v_records_rejected)
    WHERE 
        batch_id = p_batch_id AND
        source_table = 'staging.stg_order' AND
        target_table = 'integration.fact_sales_order' AND
        status = 'RUNNING';
    
    -- Return success message
    SELECT CONCAT('Sales order fact loaded successfully. Inserted: ', v_records_inserted, ', Rejected: ', v_records_rejected) AS result;
    
    -- Exception handling
    EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction
        ROLLBACK;
        
        -- Get error message
        SET v_error_message = CONCAT('Error loading sales order fact: ', SQLERRM);
        
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
            source_table = 'staging.stg_order' AND
            target_table = 'integration.fact_sales_order' AND
            status = 'RUNNING';
        
        -- Insert error log
        INSERT INTO metadata.etl_error_log (
            batch_id, source_table, target_table, error_message, record_data, created_at
        )
        VALUES (
            p_batch_id, 'staging.stg_order', 'integration.fact_sales_order', v_error_message, NULL, NOW()
        );
        
        -- Re-raise the exception
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
END //

DELIMITER ;

