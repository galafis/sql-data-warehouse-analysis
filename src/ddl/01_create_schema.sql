-- Data Warehouse Schema Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the database schemas for the Enterprise Data Warehouse

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS enterprise_data_warehouse;

-- Use the database
USE enterprise_data_warehouse;

-- Create schemas for the three-tier architecture
-- Staging Layer
CREATE SCHEMA IF NOT EXISTS staging;

-- Integration Layer (Enterprise Data Warehouse)
CREATE SCHEMA IF NOT EXISTS integration;

-- Presentation Layer (Data Marts)
CREATE SCHEMA IF NOT EXISTS sales_mart;
CREATE SCHEMA IF NOT EXISTS marketing_mart;
CREATE SCHEMA IF NOT EXISTS finance_mart;
CREATE SCHEMA IF NOT EXISTS hr_mart;
CREATE SCHEMA IF NOT EXISTS operations_mart;

-- Metadata Layer (ETL Logging and Metadata Management)
CREATE SCHEMA IF NOT EXISTS metadata;

-- Grant permissions (adjust as needed for your environment)
GRANT ALL PRIVILEGES ON staging.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON integration.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON sales_mart.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON marketing_mart.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON finance_mart.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON hr_mart.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON operations_mart.* TO 'etl_user'@'localhost';
GRANT ALL PRIVILEGES ON metadata.* TO 'etl_user'@'localhost';

GRANT SELECT ON sales_mart.* TO 'reporting_user'@'localhost';
GRANT SELECT ON marketing_mart.* TO 'reporting_user'@'localhost';
GRANT SELECT ON finance_mart.* TO 'reporting_user'@'localhost';
GRANT SELECT ON hr_mart.* TO 'reporting_user'@'localhost';
GRANT SELECT ON operations_mart.* TO 'reporting_user'@'localhost';

-- Create metadata tables for ETL logging and tracking
USE metadata;

-- ETL Log table
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

-- ETL Error Log table
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

-- ETL Batch table
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

-- Data Lineage table
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    source_column VARCHAR(100) NOT NULL,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,
    transformation_rule TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_data_lineage_source (source_system, source_table, source_column),
    INDEX idx_data_lineage_target (target_schema, target_table, target_column)
);

-- Data Dictionary table
CREATE TABLE IF NOT EXISTS data_dictionary (
    dictionary_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    description TEXT,
    business_definition TEXT,
    is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
    is_foreign_key BOOLEAN NOT NULL DEFAULT FALSE,
    referenced_table VARCHAR(100),
    referenced_column VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (schema_name, table_name, column_name),
    INDEX idx_data_dictionary_table (schema_name, table_name)
);

-- Job Schedule table
CREATE TABLE IF NOT EXISTS job_schedule (
    job_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_description TEXT,
    cron_expression VARCHAR(100) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_time TIMESTAMP NULL,
    next_run_time TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (job_name)
);

-- Print completion message
SELECT 'Enterprise Data Warehouse schemas created successfully.' AS result;

