-- ETL Master Script
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Master ETL script to orchestrate the entire data warehouse loading process

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO metadata, public;

-- Create ETL log table if not exists
CREATE TABLE IF NOT EXISTS etl_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    records_read INT NOT NULL,
    records_inserted INT NOT NULL,
    records_rejected INT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds INT GENERATED ALWAYS AS (TIMESTAMPDIFF(SECOND, start_time, end_time)) STORED,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_etl_log_batch_id (batch_id),
    INDEX idx_etl_log_start_time (start_time)
);

-- Create ETL error log table if not exists
CREATE TABLE IF NOT EXISTS etl_error_log (
    error_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    record_data TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_etl_error_log_batch_id (batch_id)
);

-- Create ETL batch table if not exists
CREATE TABLE IF NOT EXISTS etl_batch (
    batch_id VARCHAR(50) PRIMARY KEY,
    batch_type VARCHAR(50) NOT NULL,
    batch_status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INT GENERATED ALWAYS AS (TIMESTAMPDIFF(SECOND, start_time, end_time)) STORED,
    records_processed INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_etl_batch_batch_type (batch_type),
    INDEX idx_etl_batch_batch_status (batch_status),
    INDEX idx_etl_batch_start_time (start_time)
);

DELIMITER //

CREATE OR REPLACE PROCEDURE sp_run_etl_master(
    IN p_batch_type VARCHAR(50)
)
BEGIN
    DECLARE v_batch_id VARCHAR(50);
    DECLARE v_effective_date DATE;
    DECLARE v_error_message TEXT;
    DECLARE v_error_occurred BOOLEAN DEFAULT FALSE;
    
    -- Declare handler for exceptions
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_error_message = MESSAGE_TEXT;
        SET v_error_occurred = TRUE;
    END;
    
    -- Generate batch ID
    SET v_batch_id = CONCAT(p_batch_type, '_', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s'));
    
    -- Set effective date to current date
    SET v_effective_date = CURRENT_DATE();
    
    -- Start transaction
    START TRANSACTION;
    
    -- Create batch record
    INSERT INTO etl_batch (batch_id, batch_type, batch_status, start_time, records_processed)
    VALUES (v_batch_id, p_batch_type, 'RUNNING', NOW(), 0);
    
    -- Commit batch creation
    COMMIT;
    
    -- Start ETL process
    BEGIN
        -- Step 1: Generate date dimension if needed
        CALL integration.sp_generate_date_dimension(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), DATE_ADD(CURRENT_DATE(), INTERVAL 5 YEAR));
        
        -- Step 2: Load customer dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_customer',
            'staging.stg_customer',
            'CONCAT(s.source_system, '':', s.source_customer_id)',
            'd.customer_name != s.customer_name OR
             IFNULL(d.customer_type, '''') != IFNULL(s.customer_type, '''') OR
             IFNULL(d.email, '''') != IFNULL(s.email, '''') OR
             IFNULL(d.phone, '''') != IFNULL(s.phone, '''') OR
             IFNULL(d.address_line1, '''') != IFNULL(s.address_line1, '''') OR
             IFNULL(d.address_line2, '''') != IFNULL(s.address_line2, '''') OR
             IFNULL(d.city, '''') != IFNULL(s.city, '''') OR
             IFNULL(d.state_province, '''') != IFNULL(s.state_province, '''') OR
             IFNULL(d.postal_code, '''') != IFNULL(s.postal_code, '''') OR
             IFNULL(d.country, '''') != IFNULL(s.country, '''') OR
             IFNULL(d.customer_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 3: Load product dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_product',
            'staging.stg_product',
            'CONCAT(s.source_system, '':', s.source_product_id)',
            'd.product_name != s.product_name OR
             IFNULL(d.product_description, '''') != IFNULL(s.product_description, '''') OR
             IFNULL(d.category, '''') != IFNULL(s.category, '''') OR
             IFNULL(d.subcategory, '''') != IFNULL(s.subcategory, '''') OR
             IFNULL(d.brand, '''') != IFNULL(s.brand, '''') OR
             IFNULL(d.manufacturer, '''') != IFNULL(s.manufacturer, '''') OR
             IFNULL(d.sku, '''') != IFNULL(s.sku, '''') OR
             IFNULL(d.upc, '''') != IFNULL(s.upc, '''') OR
             IFNULL(d.unit_cost, 0) != IFNULL(s.unit_cost, 0) OR
             IFNULL(d.list_price, 0) != IFNULL(s.list_price, 0) OR
             IFNULL(d.product_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 4: Load store dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_store',
            'staging.stg_store',
            'CONCAT(s.source_system, '':', s.source_store_id)',
            'd.store_name != s.store_name OR
             IFNULL(d.store_type, '''') != IFNULL(s.store_type, '''') OR
             IFNULL(d.address_line1, '''') != IFNULL(s.address_line1, '''') OR
             IFNULL(d.address_line2, '''') != IFNULL(s.address_line2, '''') OR
             IFNULL(d.city, '''') != IFNULL(s.city, '''') OR
             IFNULL(d.state_province, '''') != IFNULL(s.state_province, '''') OR
             IFNULL(d.postal_code, '''') != IFNULL(s.postal_code, '''') OR
             IFNULL(d.country, '''') != IFNULL(s.country, '''') OR
             IFNULL(d.phone, '''') != IFNULL(s.phone, '''') OR
             IFNULL(d.email, '''') != IFNULL(s.email, '''') OR
             IFNULL(d.store_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 5: Load employee dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_employee',
            'staging.stg_employee',
            'CONCAT(s.source_system, '':', s.source_employee_id)',
            'd.first_name != s.first_name OR
             d.last_name != s.last_name OR
             IFNULL(d.email, '''') != IFNULL(s.email, '''') OR
             IFNULL(d.phone, '''') != IFNULL(s.phone, '''') OR
             IFNULL(d.hire_date, ''1900-01-01'') != IFNULL(s.hire_date, ''1900-01-01'') OR
             IFNULL(d.termination_date, ''9999-12-31'') != IFNULL(s.termination_date, ''9999-12-31'') OR
             IFNULL(d.job_title, '''') != IFNULL(s.job_title, '''') OR
             IFNULL(d.department, '''') != IFNULL(s.department, '''') OR
             IFNULL(d.employee_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 6: Load campaign dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_campaign',
            'staging.stg_campaign',
            'CONCAT(s.source_system, '':', s.source_campaign_id)',
            'd.campaign_name != s.campaign_name OR
             IFNULL(d.campaign_description, '''') != IFNULL(s.campaign_description, '''') OR
             IFNULL(d.campaign_type, '''') != IFNULL(s.campaign_type, '''') OR
             IFNULL(d.channel, '''') != IFNULL(s.channel, '''') OR
             IFNULL(d.start_date, ''1900-01-01'') != IFNULL(s.start_date, ''1900-01-01'') OR
             IFNULL(d.end_date, ''9999-12-31'') != IFNULL(s.end_date, ''9999-12-31'') OR
             IFNULL(d.budget, 0) != IFNULL(s.budget, 0) OR
             IFNULL(d.target_audience, '''') != IFNULL(s.target_audience, '''') OR
             IFNULL(d.goal, '''') != IFNULL(s.goal, '''') OR
             IFNULL(d.kpi, '''') != IFNULL(s.kpi, '''') OR
             IFNULL(d.campaign_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 7: Load promotion dimension
        CALL integration.sp_load_dimension_scd2(
            'dim_promotion',
            'staging.stg_promotion',
            'CONCAT(s.source_system, '':', s.source_promotion_id)',
            'd.promotion_name != s.promotion_name OR
             IFNULL(d.promotion_description, '''') != IFNULL(s.promotion_description, '''') OR
             IFNULL(d.promotion_type, '''') != IFNULL(s.promotion_type, '''') OR
             IFNULL(d.discount_type, '''') != IFNULL(s.discount_type, '''') OR
             IFNULL(d.discount_value, 0) != IFNULL(s.discount_value, 0) OR
             IFNULL(d.discount_percent, 0) != IFNULL(s.discount_percent, 0) OR
             IFNULL(d.start_date, ''1900-01-01'') != IFNULL(s.start_date, ''1900-01-01'') OR
             IFNULL(d.end_date, ''9999-12-31'') != IFNULL(s.end_date, ''9999-12-31'') OR
             IFNULL(d.promotion_status, '''') != IFNULL(s.status, '''')',
            v_effective_date,
            v_batch_id
        );
        
        -- Step 8: Load sales order fact
        CALL integration.sp_load_fact_sales_order(v_batch_id);
        
        -- Step 9: Load sales order line fact
        CALL integration.sp_load_fact_sales_order_line(v_batch_id);
        
        -- Step 10: Load inventory fact
        -- (Implementation similar to sales order fact)
        
        -- Step 11: Load campaign performance fact
        -- (Implementation similar to sales order fact)
        
        -- Step 12: Refresh presentation layer
        -- (Implementation to refresh aggregated tables in data marts)
        
        -- Update batch status to completed
        UPDATE etl_batch
        SET 
            batch_status = 'COMPLETED',
            end_time = NOW(),
            records_processed = (
                SELECT SUM(records_inserted)
                FROM etl_log
                WHERE batch_id = v_batch_id
            )
        WHERE 
            batch_id = v_batch_id;
    END;
    
    -- Handle errors
    IF v_error_occurred THEN
        UPDATE etl_batch
        SET 
            batch_status = 'FAILED',
            end_time = NOW(),
            records_processed = (
                SELECT IFNULL(SUM(records_inserted), 0)
                FROM etl_log
                WHERE batch_id = v_batch_id
            )
        WHERE 
            batch_id = v_batch_id;
        
        -- Log the error
        INSERT INTO etl_error_log (
            batch_id,
            source_table,
            target_table,
            error_message,
            record_data,
            created_at
        )
        VALUES (
            v_batch_id,
            'MASTER',
            'MASTER',
            v_error_message,
            NULL,
            NOW()
        );
        
        -- Re-throw the error
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;
    
    -- Return batch information
    SELECT 
        batch_id,
        batch_type,
        batch_status,
        start_time,
        end_time,
        duration_seconds,
        records_processed
    FROM 
        etl_batch
    WHERE 
        batch_id = v_batch_id;
END //

DELIMITER ;

