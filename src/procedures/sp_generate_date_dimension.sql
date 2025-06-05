-- Generate Date Dimension Stored Procedure
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Stored procedure to generate the date dimension table with a specified date range

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

DELIMITER //

CREATE OR REPLACE PROCEDURE sp_generate_date_dimension(
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DECLARE v_date DATE;
    DECLARE v_date_key INT;
    DECLARE v_day_of_week INT;
    DECLARE v_day_name VARCHAR(10);
    DECLARE v_day_of_month INT;
    DECLARE v_day_of_year INT;
    DECLARE v_week_of_year INT;
    DECLARE v_month_number INT;
    DECLARE v_month_name VARCHAR(10);
    DECLARE v_quarter_number INT;
    DECLARE v_quarter_name VARCHAR(10);
    DECLARE v_year_number INT;
    DECLARE v_is_weekend BOOLEAN;
    DECLARE v_is_holiday BOOLEAN;
    DECLARE v_holiday_name VARCHAR(50);
    DECLARE v_fiscal_day_of_year INT;
    DECLARE v_fiscal_week_of_year INT;
    DECLARE v_fiscal_month_number INT;
    DECLARE v_fiscal_quarter_number INT;
    DECLARE v_fiscal_year_number INT;
    
    -- Validate input parameters
    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Start date and end date must not be NULL';
    END IF;
    
    IF p_start_date > p_end_date THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Start date must be less than or equal to end date';
    END IF;
    
    -- Delete existing records if they exist in the specified range
    DELETE FROM dim_date 
    WHERE date_value BETWEEN p_start_date AND p_end_date;
    
    -- Set the current date to the start date
    SET v_date = p_start_date;
    
    -- Loop through each date in the range
    WHILE v_date <= p_end_date DO
        -- Calculate date key (YYYYMMDD format)
        SET v_date_key = YEAR(v_date) * 10000 + MONTH(v_date) * 100 + DAY(v_date);
        
        -- Calculate date attributes
        SET v_day_of_week = DAYOFWEEK(v_date);
        SET v_day_name = DAYNAME(v_date);
        SET v_day_of_month = DAYOFMONTH(v_date);
        SET v_day_of_year = DAYOFYEAR(v_date);
        SET v_week_of_year = WEEKOFYEAR(v_date);
        SET v_month_number = MONTH(v_date);
        SET v_month_name = MONTHNAME(v_date);
        SET v_quarter_number = QUARTER(v_date);
        SET v_quarter_name = CONCAT('Q', v_quarter_number);
        SET v_year_number = YEAR(v_date);
        
        -- Determine if it's a weekend
        SET v_is_weekend = (v_day_of_week = 1 OR v_day_of_week = 7);
        
        -- Initialize holiday flags
        SET v_is_holiday = FALSE;
        SET v_holiday_name = NULL;
        
        -- Check for common US holidays (simplified)
        -- New Year's Day
        IF v_month_number = 1 AND v_day_of_month = 1 THEN
            SET v_is_holiday = TRUE;
            SET v_holiday_name = 'New Year''s Day';
        END IF;
        
        -- Independence Day
        IF v_month_number = 7 AND v_day_of_month = 4 THEN
            SET v_is_holiday = TRUE;
            SET v_holiday_name = 'Independence Day';
        END IF;
        
        -- Christmas
        IF v_month_number = 12 AND v_day_of_month = 25 THEN
            SET v_is_holiday = TRUE;
            SET v_holiday_name = 'Christmas';
        END IF;
        
        -- Calculate fiscal calendar (assuming fiscal year starts July 1)
        IF v_month_number >= 7 THEN
            SET v_fiscal_year_number = v_year_number + 1;
            SET v_fiscal_month_number = v_month_number - 6;
            
            IF v_fiscal_month_number <= 3 THEN
                SET v_fiscal_quarter_number = 1;
            ELSEIF v_fiscal_month_number <= 6 THEN
                SET v_fiscal_quarter_number = 2;
            ELSEIF v_fiscal_month_number <= 9 THEN
                SET v_fiscal_quarter_number = 3;
            ELSE
                SET v_fiscal_quarter_number = 4;
            END IF;
        ELSE
            SET v_fiscal_year_number = v_year_number;
            SET v_fiscal_month_number = v_month_number + 6;
            
            IF v_fiscal_month_number <= 3 THEN
                SET v_fiscal_quarter_number = 1;
            ELSEIF v_fiscal_month_number <= 6 THEN
                SET v_fiscal_quarter_number = 2;
            ELSEIF v_fiscal_month_number <= 9 THEN
                SET v_fiscal_quarter_number = 3;
            ELSE
                SET v_fiscal_quarter_number = 4;
            END IF;
        END IF;
        
        -- Calculate fiscal day of year and week of year (simplified)
        IF v_month_number >= 7 THEN
            SET v_fiscal_day_of_year = DATEDIFF(v_date, CONCAT(v_year_number, '-07-01')) + 1;
        ELSE
            SET v_fiscal_day_of_year = DATEDIFF(v_date, CONCAT(v_year_number - 1, '-07-01')) + 1;
        END IF;
        
        SET v_fiscal_week_of_year = FLOOR((v_fiscal_day_of_year - 1) / 7) + 1;
        
        -- Insert the record into the date dimension table
        INSERT INTO dim_date (
            date_key,
            date_value,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month_number,
            month_name,
            quarter_number,
            quarter_name,
            year_number,
            is_weekend,
            is_holiday,
            holiday_name,
            fiscal_day_of_year,
            fiscal_week_of_year,
            fiscal_month_number,
            fiscal_quarter_number,
            fiscal_year_number
        ) VALUES (
            v_date_key,
            v_date,
            v_day_of_week,
            v_day_name,
            v_day_of_month,
            v_day_of_year,
            v_week_of_year,
            v_month_number,
            v_month_name,
            v_quarter_number,
            v_quarter_name,
            v_year_number,
            v_is_weekend,
            v_is_holiday,
            v_holiday_name,
            v_fiscal_day_of_year,
            v_fiscal_week_of_year,
            v_fiscal_month_number,
            v_fiscal_quarter_number,
            v_fiscal_year_number
        );
        
        -- Move to the next date
        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    END WHILE;
    
    -- Return the number of records inserted
    SELECT CONCAT('Generated date dimension from ', p_start_date, ' to ', p_end_date, 
                 '. Total records: ', DATEDIFF(p_end_date, p_start_date) + 1) AS result;
END //

DELIMITER ;

