-- Data Warehouse Staging Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the staging tables for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO staging, public;

-- Create staging tables with minimal transformation
-- These tables closely match the source system structure

-- Staging table for Customer data
CREATE TABLE IF NOT EXISTS stg_customer (
    source_system VARCHAR(50) NOT NULL,
    source_customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(50),
    email VARCHAR(255),
    phone VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    customer_since DATE,
    credit_limit DECIMAL(15, 2),
    status VARCHAR(20),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_customer_id, batch_id)
);

-- Staging table for Product data
CREATE TABLE IF NOT EXISTS stg_product (
    source_system VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    manufacturer VARCHAR(255),
    sku VARCHAR(50),
    upc VARCHAR(50),
    unit_cost DECIMAL(15, 2),
    list_price DECIMAL(15, 2),
    weight DECIMAL(10, 2),
    weight_unit VARCHAR(10),
    dimensions VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(50),
    status VARCHAR(20),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_product_id, batch_id)
);

-- Staging table for Order data
CREATE TABLE IF NOT EXISTS stg_order (
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    source_customer_id VARCHAR(50) NOT NULL,
    source_store_id VARCHAR(50),
    source_employee_id VARCHAR(50),
    order_date TIMESTAMP NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    shipping_method VARCHAR(50),
    shipping_status VARCHAR(20),
    currency_code VARCHAR(3),
    subtotal DECIMAL(15, 2),
    tax_amount DECIMAL(15, 2),
    shipping_amount DECIMAL(15, 2),
    discount_amount DECIMAL(15, 2),
    total_amount DECIMAL(15, 2),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_order_id, batch_id)
);

-- Staging table for Order Line data
CREATE TABLE IF NOT EXISTS stg_order_line (
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    source_order_line_id VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(15, 2) NOT NULL,
    unit_cost DECIMAL(15, 2),
    discount_percent DECIMAL(5, 2),
    discount_amount DECIMAL(15, 2),
    line_total DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2),
    source_promotion_id VARCHAR(50),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_order_id, source_order_line_id, batch_id)
);

-- Staging table for Store data
CREATE TABLE IF NOT EXISTS stg_store (
    source_system VARCHAR(50) NOT NULL,
    source_store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    store_type VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255),
    manager_name VARCHAR(255),
    open_date DATE,
    close_date DATE,
    status VARCHAR(20),
    size_sqft INT,
    source_region_id VARCHAR(50),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_store_id, batch_id)
);

-- Staging table for Employee data
CREATE TABLE IF NOT EXISTS stg_employee (
    source_system VARCHAR(50) NOT NULL,
    source_employee_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    hire_date DATE,
    termination_date DATE,
    job_title VARCHAR(100),
    department VARCHAR(100),
    source_manager_id VARCHAR(50),
    source_store_id VARCHAR(50),
    salary DECIMAL(15, 2),
    hourly_rate DECIMAL(10, 2),
    employment_type VARCHAR(20),
    status VARCHAR(20),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_employee_id, batch_id)
);

-- Staging table for Promotion data
CREATE TABLE IF NOT EXISTS stg_promotion (
    source_system VARCHAR(50) NOT NULL,
    source_promotion_id VARCHAR(50) NOT NULL,
    promotion_name VARCHAR(255) NOT NULL,
    promotion_description TEXT,
    promotion_type VARCHAR(50),
    discount_type VARCHAR(20),
    discount_value DECIMAL(15, 2),
    discount_percent DECIMAL(5, 2),
    start_date DATE,
    end_date DATE,
    min_purchase_amount DECIMAL(15, 2),
    min_quantity INT,
    max_quantity INT,
    status VARCHAR(20),
    source_campaign_id VARCHAR(50),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_promotion_id, batch_id)
);

-- Staging table for Inventory data
CREATE TABLE IF NOT EXISTS stg_inventory (
    source_system VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    source_store_id VARCHAR(50) NOT NULL,
    inventory_date DATE NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_on_order INT,
    quantity_in_transit INT,
    reorder_point INT,
    reorder_quantity INT,
    unit_cost DECIMAL(15, 2),
    total_value DECIMAL(15, 2),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_product_id, source_store_id, inventory_date, batch_id)
);

-- Staging table for Campaign data
CREATE TABLE IF NOT EXISTS stg_campaign (
    source_system VARCHAR(50) NOT NULL,
    source_campaign_id VARCHAR(50) NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_description TEXT,
    campaign_type VARCHAR(50),
    channel VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15, 2),
    target_audience VARCHAR(100),
    goal VARCHAR(100),
    kpi VARCHAR(100),
    status VARCHAR(20),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_campaign_id, batch_id)
);

-- Staging table for Campaign Performance data
CREATE TABLE IF NOT EXISTS stg_campaign_performance (
    source_system VARCHAR(50) NOT NULL,
    source_campaign_id VARCHAR(50) NOT NULL,
    performance_date DATE NOT NULL,
    impressions INT,
    clicks INT,
    unique_visitors INT,
    page_views INT,
    bounce_rate DECIMAL(5, 2),
    conversion_rate DECIMAL(5, 2),
    conversions INT,
    cost DECIMAL(15, 2),
    revenue DECIMAL(15, 2),
    source_last_updated TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    batch_timestamp TIMESTAMP NOT NULL,
    record_checksum VARCHAR(64) NOT NULL,
    PRIMARY KEY (source_system, source_campaign_id, performance_date, batch_id)
);

-- Create indexes for better performance
CREATE INDEX idx_stg_customer_source_last_updated ON stg_customer(source_last_updated);
CREATE INDEX idx_stg_product_source_last_updated ON stg_product(source_last_updated);
CREATE INDEX idx_stg_order_source_last_updated ON stg_order(source_last_updated);
CREATE INDEX idx_stg_order_line_source_last_updated ON stg_order_line(source_last_updated);
CREATE INDEX idx_stg_store_source_last_updated ON stg_store(source_last_updated);
CREATE INDEX idx_stg_employee_source_last_updated ON stg_employee(source_last_updated);
CREATE INDEX idx_stg_promotion_source_last_updated ON stg_promotion(source_last_updated);
CREATE INDEX idx_stg_inventory_source_last_updated ON stg_inventory(source_last_updated);
CREATE INDEX idx_stg_campaign_source_last_updated ON stg_campaign(source_last_updated);
CREATE INDEX idx_stg_campaign_performance_source_last_updated ON stg_campaign_performance(source_last_updated);

