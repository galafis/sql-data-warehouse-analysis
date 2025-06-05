-- ETL Master Script
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Master ETL script to orchestrate the data warehouse loading process

DELIMITER //

CREATE OR REPLACE PROCEDURE metadata.sp_run_etl_master(
    IN p_load_type VARCHAR(20) -- 'FULL_LOAD' or 'INCREMENTAL'
)
BEGIN
    DECLARE v_batch_id VARCHAR(50);
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_error_message TEXT;
    
    -- Generate batch ID
    SET v_batch_id = CONCAT('ETL_', p_load_type, '_', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s'));
    SET v_start_time = NOW();
    
    -- Log ETL batch start
    INSERT INTO metadata.etl_batch (
        batch_id, batch_type, batch_status, start_time, end_time, records_processed
    )
    VALUES (
        v_batch_id, p_load_type, 'RUNNING', v_start_time, NULL, 0
    );
    
    -- Begin transaction
    START TRANSACTION;
    
    -- Try to execute the ETL process
    BEGIN
        -- Step 1: Load dimensions
        -- Load Customer dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_customer', 'integration', 'dim_customer',
            'customer_id', 'customer_name,customer_type,email,phone,address_line1,address_line2,city,state_province,postal_code,country,region,status',
            v_batch_id
        );
        
        -- Load Product dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_product', 'integration', 'dim_product',
            'product_id', 'product_name,product_description,category,subcategory,brand,manufacturer,sku,upc,unit_cost,list_price,weight,weight_unit,size,color,status',
            v_batch_id
        );
        
        -- Load Store dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_store', 'integration', 'dim_store',
            'store_id', 'store_name,store_type,address_line1,address_line2,city,state_province,postal_code,country,region,phone,email,manager_name,open_date,close_date,square_footage,status',
            v_batch_id
        );
        
        -- Load Employee dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_employee', 'integration', 'dim_employee',
            'employee_id', 'first_name,last_name,email,phone,hire_date,termination_date,job_title,department,manager_id,store_id,salary,hourly_rate,status',
            v_batch_id
        );
        
        -- Load Campaign dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_campaign', 'integration', 'dim_campaign',
            'campaign_id', 'campaign_name,campaign_description,campaign_type,channel,start_date,end_date,budget,target_audience,goal,kpi,status',
            v_batch_id
        );
        
        -- Load Promotion dimension
        CALL integration.sp_load_dimension_scd2(
            'staging', 'stg_promotion', 'integration', 'dim_promotion',
            'promotion_id', 'promotion_name,promotion_description,promotion_type,discount_type,discount_value,discount_percent,start_date,end_date,minimum_order_value,maximum_discount,campaign_id,status',
            v_batch_id
        );
        
        -- Step 2: Load facts
        -- Load Sales Order fact
        CALL integration.sp_load_fact_sales_order(v_batch_id);
        
        -- Load Sales Order Line fact
        CALL integration.sp_load_fact_sales_order_line(v_batch_id);
        
        -- Step 3: Refresh data marts
        -- Refresh Sales Data Mart
        INSERT INTO sales_mart.fact_daily_sales (
            date_key,
            product_key,
            store_key,
            customer_key,
            sales_date,
            product_name,
            product_category,
            product_subcategory,
            store_name,
            store_city,
            store_state,
            store_country,
            customer_name,
            customer_type,
            customer_city,
            customer_state,
            customer_country,
            order_count,
            unit_count,
            sales_amount,
            discount_amount,
            tax_amount,
            cost_amount,
            profit_amount,
            profit_margin
        )
        SELECT
            sol.order_date_key,
            sol.product_key,
            so.store_key,
            so.customer_key,
            d.full_date,
            p.product_name,
            p.category,
            p.subcategory,
            s.store_name,
            s.city,
            s.state_province,
            s.country,
            c.customer_name,
            c.customer_type,
            c.city,
            c.state_province,
            c.country,
            COUNT(DISTINCT sol.sales_order_key),
            SUM(sol.quantity),
            SUM(sol.line_total),
            SUM(sol.discount_amount),
            SUM(sol.tax_amount),
            SUM(sol.unit_cost * sol.quantity),
            SUM(sol.profit_amount),
            CASE 
                WHEN SUM(sol.line_total) > 0 THEN SUM(sol.profit_amount) / SUM(sol.line_total) 
                ELSE 0 
            END
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
        JOIN
            integration.dim_customer c ON so.customer_key = c.customer_key
        WHERE
            so.batch_id = v_batch_id
        GROUP BY
            sol.order_date_key,
            sol.product_key,
            so.store_key,
            so.customer_key,
            d.full_date,
            p.product_name,
            p.category,
            p.subcategory,
            s.store_name,
            s.city,
            s.state_province,
            s.country,
            c.customer_name,
            c.customer_type,
            c.city,
            c.state_province,
            c.country;
        
        -- Update ETL batch with record count
        UPDATE metadata.etl_batch
        SET 
            records_processed = records_processed + ROW_COUNT()
        WHERE 
            batch_id = v_batch_id;
        
        -- Commit transaction
        COMMIT;
        
        -- Log ETL batch completion
        UPDATE metadata.etl_batch
        SET 
            batch_status = 'COMPLETED',
            end_time = NOW()
        WHERE 
            batch_id = v_batch_id;
        
        -- Return success message
        SELECT CONCAT('ETL process completed successfully. Batch ID: ', v_batch_id) AS result;
    
    -- Exception handling
    EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction
        ROLLBACK;
        
        -- Get error message
        SET v_error_message = CONCAT('Error in ETL process: ', SQLERRM);
        
        -- Log ETL batch failure
        UPDATE metadata.etl_batch
        SET 
            batch_status = 'FAILED',
            end_time = NOW()
        WHERE 
            batch_id = v_batch_id;
        
        -- Insert error log
        INSERT INTO metadata.etl_error_log (
            batch_id, source_table, target_table, error_message, record_data, created_at
        )
        VALUES (
            v_batch_id, 'MULTIPLE', 'MULTIPLE', v_error_message, NULL, NOW()
        );
        
        -- Re-raise the exception
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END //

DELIMITER ;

