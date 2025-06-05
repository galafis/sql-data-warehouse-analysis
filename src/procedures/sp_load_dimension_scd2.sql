-- Load Dimension SCD Type 2 Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Generic procedure to load SCD Type 2 dimensions from staging tables

DELIMITER //

CREATE OR REPLACE PROCEDURE integration.sp_load_dimension_scd2(
    IN p_source_schema VARCHAR(50),
    IN p_source_table VARCHAR(100),
    IN p_target_schema VARCHAR(50),
    IN p_target_table VARCHAR(100),
    IN p_business_key_columns VARCHAR(255),
    IN p_track_columns VARCHAR(1000),
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_sql TEXT;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_records_read INT DEFAULT 0;
    DECLARE v_records_inserted INT DEFAULT 0;
    DECLARE v_records_updated INT DEFAULT 0;
    DECLARE v_records_rejected INT DEFAULT 0;
    DECLARE v_error_message TEXT;
    DECLARE v_business_key_columns_list TEXT;
    DECLARE v_track_columns_list TEXT;
    DECLARE v_source_full_table VARCHAR(155);
    DECLARE v_target_full_table VARCHAR(155);
    DECLARE v_temp_table VARCHAR(100);
    
    -- Set the start time
    SET v_start_time = NOW();
    
    -- Set the full table names
    SET v_source_full_table = CONCAT(p_source_schema, '.', p_source_table);
    SET v_target_full_table = CONCAT(p_target_schema, '.', p_target_table);
    
    -- Set the temporary table name
    SET v_temp_table = CONCAT('temp_', p_target_table, '_', REPLACE(p_batch_id, '-', '_'));
    
    -- Log start of procedure
    INSERT INTO metadata.etl_log (
        batch_id, source_table, target_table, records_read, records_inserted, 
        records_rejected, start_time, end_time, status, message
    )
    VALUES (
        p_batch_id, v_source_full_table, v_target_full_table, 0, 0, 0, 
        v_start_time, NULL, 'RUNNING', 'Starting SCD Type 2 dimension load'
    );
    
    -- Replace commas with AND for the business key columns
    SET v_business_key_columns_list = REPLACE(p_business_key_columns, ',', ' AND s.');
    SET v_business_key_columns_list = CONCAT('s.', v_business_key_columns_list);
    
    -- Create a temporary table to hold the changes
    SET @drop_temp_sql = CONCAT('DROP TABLE IF EXISTS ', v_temp_table);
    PREPARE stmt FROM @drop_temp_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @create_temp_sql = CONCAT('
        CREATE TEMPORARY TABLE ', v_temp_table, ' (
            change_type VARCHAR(10),
            source_key VARCHAR(100),
            target_key BIGINT,
            effective_date DATE,
            expiration_date DATE,
            is_current BOOLEAN
        )
    ');
    PREPARE stmt FROM @create_temp_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Find new records (INSERT)
    SET @insert_new_sql = CONCAT('
        INSERT INTO ', v_temp_table, ' (change_type, source_key, target_key, effective_date, expiration_date, is_current)
        SELECT 
            ''INSERT'' AS change_type,
            CONCAT(s.source_system, ''_'', s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ') AS source_key,
            NULL AS target_key,
            CURRENT_DATE() AS effective_date,
            ''9999-12-31'' AS expiration_date,
            TRUE AS is_current
        FROM 
            ', v_source_full_table, ' s
        LEFT JOIN 
            ', v_target_full_table, ' t
        ON 
            ', v_business_key_columns_list, ' = t.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), '
            AND t.source_system = s.source_system
            AND t.is_current = TRUE
        WHERE 
            t.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ' IS NULL
            AND s.batch_id = ''', p_batch_id, '''
    ');
    PREPARE stmt FROM @insert_new_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Get the count of new records
    SET @count_new_sql = CONCAT('
        SELECT COUNT(*) INTO @new_count
        FROM ', v_temp_table, '
        WHERE change_type = ''INSERT''
    ');
    PREPARE stmt FROM @count_new_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Add the new records count to the total
    SET v_records_inserted = v_records_inserted + @new_count;
    
    -- Find changed records (UPDATE)
    SET @insert_changed_sql = CONCAT('
        INSERT INTO ', v_temp_table, ' (change_type, source_key, target_key, effective_date, expiration_date, is_current)
        SELECT 
            ''UPDATE'' AS change_type,
            CONCAT(s.source_system, ''_'', s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ') AS source_key,
            t.', SUBSTRING_INDEX(p_target_table, '_', -1), '_key AS target_key,
            CURRENT_DATE() AS effective_date,
            ''9999-12-31'' AS expiration_date,
            TRUE AS is_current
        FROM 
            ', v_source_full_table, ' s
        JOIN 
            ', v_target_full_table, ' t
        ON 
            ', v_business_key_columns_list, ' = t.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), '
            AND t.source_system = s.source_system
            AND t.is_current = TRUE
        WHERE 
            s.batch_id = ''', p_batch_id, '''
            AND (
                ', REPLACE(REPLACE(p_track_columns, ',', ' OR s.'), ' ', ' <> t.'), '
            )
    ');
    PREPARE stmt FROM @insert_changed_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Get the count of changed records
    SET @count_changed_sql = CONCAT('
        SELECT COUNT(*) INTO @changed_count
        FROM ', v_temp_table, '
        WHERE change_type = ''UPDATE''
    ');
    PREPARE stmt FROM @count_changed_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Add the changed records count to the total
    SET v_records_updated = v_records_updated + @changed_count;
    
    -- Begin transaction
    START TRANSACTION;
    
    -- Update current records to set is_current = FALSE and expiration_date = CURRENT_DATE() - 1
    SET @update_current_sql = CONCAT('
        UPDATE ', v_target_full_table, ' t
        JOIN ', v_temp_table, ' c
        ON t.', SUBSTRING_INDEX(p_target_table, '_', -1), '_key = c.target_key
        SET 
            t.is_current = FALSE,
            t.expiration_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        WHERE 
            c.change_type = ''UPDATE''
            AND t.is_current = TRUE
    ');
    PREPARE stmt FROM @update_current_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Insert new versions of changed records
    SET @insert_changed_versions_sql = CONCAT('
        INSERT INTO ', v_target_full_table, ' (
            source_system,
            source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ',
            ', p_track_columns, ',
            effective_date,
            expiration_date,
            is_current,
            batch_id,
            created_at
        )
        SELECT 
            s.source_system,
            s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ',
            ', REPLACE(p_track_columns, ',', ', s.'), ',
            c.effective_date,
            c.expiration_date,
            c.is_current,
            s.batch_id,
            NOW()
        FROM 
            ', v_source_full_table, ' s
        JOIN 
            ', v_temp_table, ' c
        ON 
            CONCAT(s.source_system, ''_'', s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ') = c.source_key
        WHERE 
            c.change_type = ''UPDATE''
            AND s.batch_id = ''', p_batch_id, '''
    ');
    PREPARE stmt FROM @insert_changed_versions_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Insert new records
    SET @insert_new_records_sql = CONCAT('
        INSERT INTO ', v_target_full_table, ' (
            source_system,
            source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ',
            ', p_track_columns, ',
            effective_date,
            expiration_date,
            is_current,
            batch_id,
            created_at
        )
        SELECT 
            s.source_system,
            s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ',
            ', REPLACE(p_track_columns, ',', ', s.'), ',
            c.effective_date,
            c.expiration_date,
            c.is_current,
            s.batch_id,
            NOW()
        FROM 
            ', v_source_full_table, ' s
        JOIN 
            ', v_temp_table, ' c
        ON 
            CONCAT(s.source_system, ''_'', s.source_', SUBSTRING_INDEX(p_business_key_columns, ',', 1), ') = c.source_key
        WHERE 
            c.change_type = ''INSERT''
            AND s.batch_id = ''', p_batch_id, '''
    ');
    PREPARE stmt FROM @insert_new_records_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Commit transaction
    COMMIT;
    
    -- Get the total count of records processed
    SET @count_total_sql = CONCAT('
        SELECT COUNT(*) INTO @total_count
        FROM ', v_source_full_table, '
        WHERE batch_id = ''', p_batch_id, '''
    ');
    PREPARE stmt FROM @count_total_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET v_records_read = @total_count;
    
    -- Drop the temporary table
    SET @drop_temp_sql = CONCAT('DROP TABLE IF EXISTS ', v_temp_table);
    PREPARE stmt FROM @drop_temp_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Log completion of procedure
    UPDATE metadata.etl_log
    SET 
        end_time = NOW(),
        records_read = v_records_read,
        records_inserted = v_records_inserted,
        records_rejected = v_records_rejected,
        status = 'COMPLETED',
        message = CONCAT('Successfully loaded SCD Type 2 dimension. New: ', v_records_inserted, ', Updated: ', v_records_updated)
    WHERE 
        batch_id = p_batch_id AND
        source_table = v_source_full_table AND
        target_table = v_target_full_table AND
        status = 'RUNNING';
    
    -- Return success message
    SELECT CONCAT('SCD Type 2 dimension loaded successfully. New: ', v_records_inserted, ', Updated: ', v_records_updated) AS result;
    
    -- Exception handling
    EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction
        ROLLBACK;
        
        -- Get error message
        SET v_error_message = CONCAT('Error loading SCD Type 2 dimension: ', SQLERRM);
        
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
            source_table = v_source_full_table AND
            target_table = v_target_full_table AND
            status = 'RUNNING';
        
        -- Insert error log
        INSERT INTO metadata.etl_error_log (
            batch_id, source_table, target_table, error_message, record_data, created_at
        )
        VALUES (
            p_batch_id, v_source_full_table, v_target_full_table, v_error_message, NULL, NOW()
        );
        
        -- Re-raise the exception
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
END //

DELIMITER ;

