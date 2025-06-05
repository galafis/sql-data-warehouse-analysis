-- Load Sales Order Fact Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Stored procedure to load the sales order fact table from staging

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

DELIMITER //

CREATE OR REPLACE PROCEDURE sp_load_fact_sales_order(
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
    INSERT INTO fact_sales_order (
        order_date_key,
        customer_key,
        store_key,
        employee_key,
        payment_method_key,
        shipping_method_key,
        currency_key,
        business_key,
        source_system,
        source_order_id,
        order_status,
        payment_status,
        shipping_status,
        subtotal_amount,
        tax_amount,
        shipping_amount,
        discount_amount,
        total_amount,
        order_line_count,
        total_quantity
    )
    SELECT 
        dd.date_key,
        dc.customer_key,
        ds.store_key,
        de.employee_key,
        dpm.payment_method_key,
        dsm.shipping_method_key,
        dcur.currency_key,
        CONCAT(s.source_system, ':', s.source_order_id),
        s.source_system,
        s.source_order_id,
        s.order_status,
        s.payment_status,
        s.shipping_status,
        s.subtotal,
        s.tax_amount,
        s.shipping_amount,
        s.discount_amount,
        s.total_amount,
        COUNT(ol.source_order_line_id),
        SUM(ol.quantity)
    FROM 
        staging.stg_order s
    JOIN 
        dim_date dd ON dd.date_key = YEAR(s.order_date) * 10000 + MONTH(s.order_date) * 100 + DAY(s.order_date)
    JOIN 
        dim_customer dc ON CONCAT(s.source_system, ':', s.source_customer_id) = dc.business_key AND dc.is_current = TRUE
    LEFT JOIN 
        dim_store ds ON CONCAT(s.source_system, ':', s.source_store_id) = ds.business_key AND ds.is_current = TRUE
    LEFT JOIN 
        dim_employee de ON CONCAT(s.source_system, ':', s.source_employee_id) = de.business_key AND de.is_current = TRUE
    JOIN 
        dim_payment_method dpm ON dpm.payment_method_code = s.payment_method
    LEFT JOIN 
        dim_shipping_method dsm ON dsm.shipping_method_code = s.shipping_method
    JOIN 
        dim_currency dcur ON dcur.currency_code = s.currency_code
    JOIN 
        staging.stg_order_line ol ON s.source_system = ol.source_system AND s.source_order_id = ol.source_order_id AND ol.batch_id = p_batch_id
    WHERE 
        s.batch_id = p_batch_id
        AND NOT EXISTS (
            SELECT 1 
            FROM fact_sales_order fso 
            WHERE fso.business_key = CONCAT(s.source_system, ':', s.source_order_id)
        )
    GROUP BY
        dd.date_key,
        dc.customer_key,
        ds.store_key,
        de.employee_key,
        dpm.payment_method_key,
        dsm.shipping_method_key,
        dcur.currency_key,
        CONCAT(s.source_system, ':', s.source_order_id),
        s.source_system,
        s.source_order_id,
        s.order_status,
        s.payment_status,
        s.shipping_status,
        s.subtotal,
        s.tax_amount,
        s.shipping_amount,
        s.discount_amount,
        s.total_amount;
    
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
        'stg_order',
        'fact_sales_order',
        CONCAT('Missing dimension key for: ', 
               CASE 
                   WHEN dd.date_key IS NULL THEN 'date_key' 
                   WHEN dc.customer_key IS NULL THEN 'customer_key'
                   WHEN dpm.payment_method_key IS NULL THEN 'payment_method_key'
                   WHEN dcur.currency_key IS NULL THEN 'currency_key'
                   ELSE 'unknown'
               END),
        CONCAT('source_order_id: ', s.source_order_id),
        NOW()
    FROM 
        staging.stg_order s
    LEFT JOIN 
        dim_date dd ON dd.date_key = YEAR(s.order_date) * 10000 + MONTH(s.order_date) * 100 + DAY(s.order_date)
    LEFT JOIN 
        dim_customer dc ON CONCAT(s.source_system, ':', s.source_customer_id) = dc.business_key AND dc.is_current = TRUE
    LEFT JOIN 
        dim_payment_method dpm ON dpm.payment_method_code = s.payment_method
    LEFT JOIN 
        dim_currency dcur ON dcur.currency_code = s.currency_code
    WHERE 
        s.batch_id = p_batch_id
        AND NOT EXISTS (
            SELECT 1 
            FROM fact_sales_order fso 
            WHERE fso.business_key = CONCAT(s.source_system, ':', s.source_order_id)
        )
        AND (dd.date_key IS NULL OR dc.customer_key IS NULL OR dpm.payment_method_key IS NULL OR dcur.currency_key IS NULL);
    
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
        'stg_order',
        'fact_sales_order',
        v_records_inserted + v_records_rejected,
        v_records_inserted,
        v_records_rejected,
        NOW(),
        NOW(),
        'COMPLETED',
        CONCAT('Successfully loaded ', v_records_inserted, ' records into fact_sales_order. ', 
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

