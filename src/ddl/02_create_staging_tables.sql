-- Data Warehouse Staging Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the staging tables for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO staging, public;

-- Customer staging table
CREATE TABLE IF NOT EXISTS stg_customer (
    customer_id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    region VARCHAR(100),
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_customer_id, batch_id),
    INDEX idx_stg_customer_batch_id (batch_id)
);

-- Product staging table
CREATE TABLE IF NOT EXISTS stg_product (
    product_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    manufacturer VARCHAR(100),
    sku VARCHAR(50),
    upc VARCHAR(50),
    unit_cost DECIMAL(15, 2),
    list_price DECIMAL(15, 2),
    weight DECIMAL(10, 2),
    weight_unit VARCHAR(10),
    size VARCHAR(50),
    color VARCHAR(50),
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_product_id, batch_id),
    INDEX idx_stg_product_batch_id (batch_id)
);

-- Store staging table
CREATE TABLE IF NOT EXISTS stg_store (
    store_id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    region VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255),
    manager_name VARCHAR(255),
    open_date DATE,
    close_date DATE,
    square_footage INT,
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_store_id, batch_id),
    INDEX idx_stg_store_batch_id (batch_id)
);

-- Employee staging table
CREATE TABLE IF NOT EXISTS stg_employee (
    employee_id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    manager_id VARCHAR(50),
    store_id VARCHAR(50),
    salary DECIMAL(15, 2),
    hourly_rate DECIMAL(10, 2),
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_employee_id, batch_id),
    INDEX idx_stg_employee_batch_id (batch_id)
);

-- Campaign staging table
CREATE TABLE IF NOT EXISTS stg_campaign (
    campaign_id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    goal VARCHAR(255),
    kpi VARCHAR(100),
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_campaign_id, batch_id),
    INDEX idx_stg_campaign_batch_id (batch_id)
);

-- Promotion staging table
CREATE TABLE IF NOT EXISTS stg_promotion (
    promotion_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_promotion_id VARCHAR(50) NOT NULL,
    promotion_name VARCHAR(255) NOT NULL,
    promotion_description TEXT,
    promotion_type VARCHAR(50),
    discount_type VARCHAR(50),
    discount_value DECIMAL(15, 2),
    discount_percent DECIMAL(5, 2),
    start_date DATE,
    end_date DATE,
    minimum_order_value DECIMAL(15, 2),
    maximum_discount DECIMAL(15, 2),
    campaign_id VARCHAR(50),
    status VARCHAR(50),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_promotion_id, batch_id),
    INDEX idx_stg_promotion_batch_id (batch_id)
);

-- Order staging table
CREATE TABLE IF NOT EXISTS stg_order (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    source_customer_id VARCHAR(50) NOT NULL,
    source_store_id VARCHAR(50),
    source_employee_id VARCHAR(50),
    order_status VARCHAR(50) NOT NULL,
    payment_status VARCHAR(50),
    shipping_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    currency_code VARCHAR(3),
    subtotal DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2),
    shipping_amount DECIMAL(15, 2),
    discount_amount DECIMAL(15, 2),
    total_amount DECIMAL(15, 2) NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_order_id, batch_id),
    INDEX idx_stg_order_batch_id (batch_id)
);

-- Order Line staging table
CREATE TABLE IF NOT EXISTS stg_order_line (
    order_line_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    source_order_line_id VARCHAR(50) NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    source_promotion_id VARCHAR(50),
    quantity INT NOT NULL,
    unit_price DECIMAL(15, 2) NOT NULL,
    unit_cost DECIMAL(15, 2),
    discount_percent DECIMAL(5, 2),
    discount_amount DECIMAL(15, 2),
    tax_amount DECIMAL(15, 2),
    line_total DECIMAL(15, 2) NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_order_id, source_order_line_id, batch_id),
    INDEX idx_stg_order_line_batch_id (batch_id)
);

-- Inventory staging table
CREATE TABLE IF NOT EXISTS stg_inventory (
    inventory_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_inventory_id VARCHAR(50) NOT NULL,
    inventory_date DATE NOT NULL,
    source_product_id VARCHAR(50) NOT NULL,
    source_store_id VARCHAR(50) NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_on_order INT,
    quantity_in_transit INT,
    quantity_reserved INT,
    quantity_available INT,
    reorder_point INT,
    reorder_quantity INT,
    unit_cost DECIMAL(15, 2),
    total_value DECIMAL(15, 2),
    days_of_supply INT,
    is_stock_out BOOLEAN,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_inventory_id, batch_id),
    INDEX idx_stg_inventory_batch_id (batch_id)
);

-- Campaign Performance staging table
CREATE TABLE IF NOT EXISTS stg_campaign_performance (
    campaign_performance_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_campaign_performance_id VARCHAR(50) NOT NULL,
    performance_date DATE NOT NULL,
    source_campaign_id VARCHAR(50) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    impressions INT,
    clicks INT,
    unique_visitors INT,
    page_views INT,
    bounce_rate DECIMAL(5, 2),
    conversion_rate DECIMAL(5, 2),
    conversions INT,
    cost DECIMAL(15, 2),
    revenue DECIMAL(15, 2),
    roi DECIMAL(15, 4),
    ctr DECIMAL(15, 4),
    cost_per_click DECIMAL(15, 4),
    cost_per_acquisition DECIMAL(15, 4),
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, source_campaign_performance_id, batch_id),
    INDEX idx_stg_campaign_performance_batch_id (batch_id)
);

-- Print completion message
SELECT 'Enterprise Data Warehouse staging tables created successfully.' AS result;

