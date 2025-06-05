-- Load Dimension SCD Type 2 Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Generic stored procedure to load SCD Type 2 dimensions

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

DELIMITER //

CREATE OR REPLACE PROCEDURE sp_load_dimension_scd2(
    IN p_dimension_name VARCHAR(100),
    IN p_staging_table VARCHAR(100),
    IN p_business_key_columns VARCHAR(255),
    IN p_change_tracking_columns VARCHAR(1000),
    IN p_effective_date DATE,
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_sql_expire TEXT;
    DECLARE v_sql_insert_changed TEXT;
    DECLARE v_sql_insert_new TEXT;
    DECLARE v_sql_columns TEXT;
    DECLARE v_sql_values TEXT;
    DECLARE v_expiration_date DATE;
    DECLARE v_source_system VARCHAR(50);
    
    -- Set variables
    SET v_expiration_date = '9999-12-31';
    SET v_source_system = 'ERP_SYSTEM';
    
    -- Get column list for the dimension table (excluding surrogate key and metadata columns)
    SELECT GROUP_CONCAT(COLUMN_NAME)
    INTO v_sql_columns
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'integration'
      AND TABLE_NAME = p_dimension_name
      AND COLUMN_NAME NOT IN (CONCAT(SUBSTRING(p_dimension_name, 5), '_key'), 'created_at', 'updated_at')
    ORDER BY ORDINAL_POSITION;
    
    -- Get corresponding values list from staging table
    SELECT GROUP_CONCAT(
        CASE 
            WHEN COLUMN_NAME = 'effective_date' THEN 'p_effective_date'
            WHEN COLUMN_NAME = 'expiration_date' THEN 'v_expiration_date'
            WHEN COLUMN_NAME = 'is_current' THEN 'TRUE'
            WHEN COLUMN_NAME = 'version' THEN '1'
            WHEN COLUMN_NAME = 'business_key' THEN CONCAT('CONCAT(', p_business_key_columns, ')')
            ELSE CONCAT('s.', COLUMN_NAME)
        END
    )
    INTO v_sql_values
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'integration'
      AND TABLE_NAME = p_dimension_name
      AND COLUMN_NAME NOT IN (CONCAT(SUBSTRING(p_dimension_name, 5), '_key'), 'created_at', 'updated_at')
    ORDER BY ORDINAL_POSITION;
    
    -- Build SQL to expire current records that have changed
    SET v_sql_expire = CONCAT('
        UPDATE ', p_dimension_name, ' d
        JOIN ', p_staging_table, ' s ON ', p_business_key_columns, ' = d.business_key
        SET 
            d.expiration_date = DATE_SUB(p_effective_date, INTERVAL 1 DAY),
            d.is_current = FALSE
        WHERE 
            d.is_current = TRUE
            AND s.batch_id = ''', p_batch_id, '''
            AND (', p_change_tracking_columns, ')
    ');
    
    -- Build SQL to insert new records for changed dimensions
    SET v_sql_insert_changed = CONCAT('
        INSERT INTO ', p_dimension_name, ' (', v_sql_columns, ')
        SELECT ', v_sql_values, '
        FROM ', p_staging_table, ' s
        JOIN ', p_dimension_name, ' d ON ', p_business_key_columns, ' = d.business_key
        WHERE 
            d.expiration_date = DATE_SUB(p_effective_date, INTERVAL 1 DAY)
            AND d.is_current = FALSE
            AND s.batch_id = ''', p_batch_id, '''
    ');
    
    -- Build SQL to insert new records
    SET v_sql_insert_new = CONCAT('
        INSERT INTO ', p_dimension_name, ' (', v_sql_columns, ')
        SELECT ', v_sql_values, '
        FROM ', p_staging_table, ' s
        LEFT JOIN ', p_dimension_name, ' d ON ', p_business_key_columns, ' = d.business_key
        WHERE 
            d.business_key IS NULL
            AND s.batch_id = ''', p_batch_id, '''
    ');
    
    -- Start transaction
    START TRANSACTION;
    
    -- Execute the SQL statements
    SET @sql = v_sql_expire;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = v_sql_insert_changed;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = v_sql_insert_new;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Commit transaction
    COMMIT;
    
    -- Return the number of records processed
    SELECT 
        p_dimension_name AS dimension_name,
        (SELECT COUNT(*) FROM integration.dim_customer WHERE effective_date = p_effective_date) AS records_inserted,
        (SELECT COUNT(*) FROM integration.dim_customer WHERE expiration_date = DATE_SUB(p_effective_date, INTERVAL 1 DAY)) AS records_expired,
        (SELECT COUNT(*) FROM integration.dim_customer WHERE is_current = TRUE) AS current_records;
END //

DELIMITER ;

