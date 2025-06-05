-- Data Warehouse Integration Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the integration layer tables for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

-- Date dimension table
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_month INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_week INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,
    year_number INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    holiday_name VARCHAR(50),
    fiscal_month INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    fiscal_year INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_date_full_date (full_date),
    INDEX idx_dim_date_year_month (year_number, month_number)
);

-- Customer dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_customer_source (source_system, source_customer_id),
    INDEX idx_dim_customer_current (is_current),
    INDEX idx_dim_customer_dates (effective_date, expiration_date)
);

-- Product dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_product_source (source_system, source_product_id),
    INDEX idx_dim_product_current (is_current),
    INDEX idx_dim_product_dates (effective_date, expiration_date)
);

-- Store dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_store (
    store_key BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_store_source (source_system, source_store_id),
    INDEX idx_dim_store_current (is_current),
    INDEX idx_dim_store_dates (effective_date, expiration_date)
);

-- Employee dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_employee_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(201) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
    email VARCHAR(255),
    phone VARCHAR(50),
    hire_date DATE,
    termination_date DATE,
    job_title VARCHAR(100),
    department VARCHAR(100),
    manager_id VARCHAR(50),
    store_key BIGINT,
    salary DECIMAL(15, 2),
    hourly_rate DECIMAL(10, 2),
    status VARCHAR(50),
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_employee_source (source_system, source_employee_id),
    INDEX idx_dim_employee_current (is_current),
    INDEX idx_dim_employee_dates (effective_date, expiration_date)
);

-- Campaign dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_campaign_source (source_system, source_campaign_id),
    INDEX idx_dim_campaign_current (is_current),
    INDEX idx_dim_campaign_dates (effective_date, expiration_date)
);

-- Promotion dimension table with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_promotion (
    promotion_key BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    campaign_key BIGINT,
    status VARCHAR(50),
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    source_created_date TIMESTAMP,
    source_updated_date TIMESTAMP,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dim_promotion_source (source_system, source_promotion_id),
    INDEX idx_dim_promotion_current (is_current),
    INDEX idx_dim_promotion_dates (effective_date, expiration_date)
);

-- Payment Method dimension table
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    payment_method_code VARCHAR(50) NOT NULL,
    payment_method_name VARCHAR(100) NOT NULL,
    payment_method_description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (payment_method_code)
);

-- Shipping Method dimension table
CREATE TABLE IF NOT EXISTS dim_shipping_method (
    shipping_method_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    shipping_method_code VARCHAR(50) NOT NULL,
    shipping_method_name VARCHAR(100) NOT NULL,
    shipping_method_description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (shipping_method_code)
);

-- Currency dimension table
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    currency_symbol VARCHAR(10),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (currency_code)
);

-- Sales Order fact table
CREATE TABLE IF NOT EXISTS fact_sales_order (
    sales_order_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_date_key INT NOT NULL,
    customer_key BIGINT NOT NULL,
    store_key BIGINT,
    employee_key BIGINT,
    payment_method_key BIGINT,
    shipping_method_key BIGINT,
    currency_key BIGINT,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    payment_status VARCHAR(50),
    shipping_status VARCHAR(50),
    subtotal DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2),
    shipping_amount DECIMAL(15, 2),
    discount_amount DECIMAL(15, 2),
    total_amount DECIMAL(15, 2) NOT NULL,
    order_line_count INT NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fact_sales_order_date (order_date_key),
    INDEX idx_fact_sales_order_customer (customer_key),
    INDEX idx_fact_sales_order_store (store_key),
    INDEX idx_fact_sales_order_employee (employee_key),
    INDEX idx_fact_sales_order_source (source_system, source_order_id)
) PARTITION BY RANGE (order_date_key) (
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Sales Order Line fact table
CREATE TABLE IF NOT EXISTS fact_sales_order_line (
    sales_order_line_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    sales_order_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    promotion_key BIGINT,
    order_date_key INT NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    source_order_line_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(15, 2) NOT NULL,
    unit_cost DECIMAL(15, 2),
    discount_percent DECIMAL(5, 2),
    discount_amount DECIMAL(15, 2),
    tax_amount DECIMAL(15, 2),
    line_total DECIMAL(15, 2) NOT NULL,
    profit_amount DECIMAL(15, 2) GENERATED ALWAYS AS (line_total - (unit_cost * quantity)) STORED,
    profit_margin DECIMAL(15, 4) GENERATED ALWAYS AS ((line_total - (unit_cost * quantity)) / line_total) STORED,
    batch_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fact_sales_order_line_order (sales_order_key),
    INDEX idx_fact_sales_order_line_product (product_key),
    INDEX idx_fact_sales_order_line_promotion (promotion_key),
    INDEX idx_fact_sales_order_line_date (order_date_key),
    INDEX idx_fact_sales_order_line_source (source_system, source_order_id, source_order_line_id)
) PARTITION BY RANGE (order_date_key) (
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Inventory fact table
CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_inventory_id VARCHAR(50) NOT NULL,
    inventory_date DATE NOT NULL,
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
    INDEX idx_fact_inventory_date (date_key),
    INDEX idx_fact_inventory_product (product_key),
    INDEX idx_fact_inventory_store (store_key),
    INDEX idx_fact_inventory_source (source_system, source_inventory_id)
) PARTITION BY RANGE (date_key) (
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Campaign Performance fact table
CREATE TABLE IF NOT EXISTS fact_campaign_performance (
    campaign_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    campaign_key BIGINT NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_campaign_performance_id VARCHAR(50) NOT NULL,
    performance_date DATE NOT NULL,
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
    INDEX idx_fact_campaign_performance_date (date_key),
    INDEX idx_fact_campaign_performance_campaign (campaign_key),
    INDEX idx_fact_campaign_performance_source (source_system, source_campaign_performance_id)
) PARTITION BY RANGE (date_key) (
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Print completion message
SELECT 'Enterprise Data Warehouse integration tables created successfully.' AS result;

