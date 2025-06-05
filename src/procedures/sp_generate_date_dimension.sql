-- Generate Date Dimension Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Generates the date dimension table with calendar attributes

DELIMITER //

CREATE OR REPLACE PROCEDURE integration.sp_generate_date_dimension(
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DECLARE v_date DATE;
    DECLARE v_first_day_of_fiscal_year DATE;
    DECLARE v_batch_id VARCHAR(50);
    
    -- Generate batch ID
    SET v_batch_id = CONCAT('DATE_DIM_', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s'));
    
    -- Log start of procedure
    INSERT INTO metadata.etl_log (
        batch_id, source_table, target_table, records_read, records_inserted, 
        records_rejected, start_time, end_time, status, message
    )
    VALUES (
        v_batch_id, 'SYSTEM', 'integration.dim_date', 0, 0, 0, 
        NOW(), NULL, 'RUNNING', 'Starting date dimension generation'
    );
    
    -- Set the date to the start date
    SET v_date = p_start_date;
    
    -- Loop through each date
    WHILE v_date <= p_end_date DO
        -- Calculate fiscal year start (assuming fiscal year starts on July 1)
        IF MONTH(v_date) >= 7 THEN
            SET v_first_day_of_fiscal_year = CONCAT(YEAR(v_date), '-07-01');
        ELSE
            SET v_first_day_of_fiscal_year = CONCAT(YEAR(v_date) - 1, '-07-01');
        END IF;
        
        -- Insert or update the date record
        INSERT INTO integration.dim_date (
            date_key,
            full_date,
            day_of_month,
            day_name,
            day_of_week,
            day_of_year,
            week_of_year,
            month_number,
            month_name,
            quarter,
            quarter_name,
            year_number,
            is_weekend,
            is_holiday,
            holiday_name,
            fiscal_month,
            fiscal_quarter,
            fiscal_year
        )
        VALUES (
            -- date_key (YYYYMMDD format)
            YEAR(v_date) * 10000 + MONTH(v_date) * 100 + DAY(v_date),
            -- full_date
            v_date,
            -- day_of_month
            DAY(v_date),
            -- day_name
            DAYNAME(v_date),
            -- day_of_week (1 = Sunday, 7 = Saturday)
            DAYOFWEEK(v_date),
            -- day_of_year
            DAYOFYEAR(v_date),
            -- week_of_year
            WEEK(v_date, 3),
            -- month_number
            MONTH(v_date),
            -- month_name
            MONTHNAME(v_date),
            -- quarter
            QUARTER(v_date),
            -- quarter_name
            CONCAT('Q', QUARTER(v_date)),
            -- year_number
            YEAR(v_date),
            -- is_weekend
            CASE WHEN DAYOFWEEK(v_date) IN (1, 7) THEN TRUE ELSE FALSE END,
            -- is_holiday (simplified logic - would need more complex logic for actual holidays)
            CASE 
                WHEN (MONTH(v_date) = 1 AND DAY(v_date) = 1) OR -- New Year's Day
                     (MONTH(v_date) = 7 AND DAY(v_date) = 4) OR -- Independence Day
                     (MONTH(v_date) = 12 AND DAY(v_date) = 25)  -- Christmas
                THEN TRUE 
                ELSE FALSE 
            END,
            -- holiday_name
            CASE 
                WHEN (MONTH(v_date) = 1 AND DAY(v_date) = 1) THEN 'New Year''s Day'
                WHEN (MONTH(v_date) = 7 AND DAY(v_date) = 4) THEN 'Independence Day'
                WHEN (MONTH(v_date) = 12 AND DAY(v_date) = 25) THEN 'Christmas'
                ELSE NULL
            END,
            -- fiscal_month (assuming fiscal year starts on July 1)
            CASE
                WHEN MONTH(v_date) >= 7 THEN MONTH(v_date) - 6
                ELSE MONTH(v_date) + 6
            END,
            -- fiscal_quarter (assuming fiscal year starts on July 1)
            CASE
                WHEN MONTH(v_date) BETWEEN 7 AND 9 THEN 1
                WHEN MONTH(v_date) BETWEEN 10 AND 12 THEN 2
                WHEN MONTH(v_date) BETWEEN 1 AND 3 THEN 3
                WHEN MONTH(v_date) BETWEEN 4 AND 6 THEN 4
            END,
            -- fiscal_year (assuming fiscal year starts on July 1)
            CASE
                WHEN MONTH(v_date) >= 7 THEN YEAR(v_date) + 1
                ELSE YEAR(v_date)
            END
        )
        ON DUPLICATE KEY UPDATE
            day_name = VALUES(day_name),
            is_weekend = VALUES(is_weekend),
            is_holiday = VALUES(is_holiday),
            holiday_name = VALUES(holiday_name);
        
        -- Move to the next date
        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    END WHILE;
    
    -- Log completion of procedure
    UPDATE metadata.etl_log
    SET 
        end_time = NOW(),
        records_read = DATEDIFF(p_end_date, p_start_date) + 1,
        records_inserted = DATEDIFF(p_end_date, p_start_date) + 1,
        records_rejected = 0,
        status = 'COMPLETED',
        message = CONCAT('Successfully generated date dimension from ', p_start_date, ' to ', p_end_date)
    WHERE 
        batch_id = v_batch_id AND
        source_table = 'SYSTEM' AND
        target_table = 'integration.dim_date';
    
    -- Return success message
    SELECT CONCAT('Date dimension generated successfully from ', p_start_date, ' to ', p_end_date) AS result;
END //

DELIMITER ;

