-- Data Warehouse Integration Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the integration layer tables for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;
SET search_path TO integration, public;

-- Create integration tables with business keys and surrogate keys
-- These tables represent the normalized enterprise data model

-- Create Date dimension table
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    date_value DATE NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month TINYINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    week_of_year TINYINT NOT NULL,
    month_number TINYINT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number TINYINT NOT NULL,
    quarter_name VARCHAR(10) NOT NULL,
    year_number SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    holiday_name VARCHAR(50),
    fiscal_day_of_year SMALLINT,
    fiscal_week_of_year TINYINT,
    fiscal_month_number TINYINT,
    fiscal_quarter_number TINYINT,
    fiscal_year_number SMALLINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (date_value)
) PARTITION BY RANGE (year_number) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create Customer dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    customer_since DATE,
    credit_limit DECIMAL(15, 2),
    customer_status VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date)
);

-- Create Product dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    product_status VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date)
);

-- Create Store dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_store (
    store_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    store_status VARCHAR(20) NOT NULL,
    size_sqft INT,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date)
);

-- Create Employee dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    manager_key INT,
    store_key INT,
    salary DECIMAL(15, 2),
    hourly_rate DECIMAL(10, 2),
    employment_type VARCHAR(20),
    employee_status VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date),
    FOREIGN KEY (manager_key) REFERENCES dim_employee(employee_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
);

-- Create Promotion dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_promotion (
    promotion_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    promotion_status VARCHAR(20) NOT NULL,
    campaign_key INT,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date)
);

-- Create Campaign dimension table (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key INT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(100) NOT NULL,
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
    campaign_status VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key, effective_date)
);

-- Create Payment Method dimension table
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key INT AUTO_INCREMENT PRIMARY KEY,
    payment_method_code VARCHAR(50) NOT NULL,
    payment_method_name VARCHAR(100) NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (payment_method_code)
);

-- Create Shipping Method dimension table
CREATE TABLE IF NOT EXISTS dim_shipping_method (
    shipping_method_key INT AUTO_INCREMENT PRIMARY KEY,
    shipping_method_code VARCHAR(50) NOT NULL,
    shipping_method_name VARCHAR(100) NOT NULL,
    carrier_name VARCHAR(100),
    description VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (shipping_method_code)
);

-- Create Currency dimension table
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key INT AUTO_INCREMENT PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    symbol VARCHAR(10),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (currency_code)
);

-- Create Sales Order fact table
CREATE TABLE IF NOT EXISTS fact_sales_order (
    sales_order_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_date_key INT NOT NULL,
    customer_key INT NOT NULL,
    store_key INT,
    employee_key INT,
    payment_method_key INT,
    shipping_method_key INT,
    currency_key INT NOT NULL,
    business_key VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    payment_status VARCHAR(20),
    shipping_status VARCHAR(20),
    subtotal_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    shipping_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    total_amount DECIMAL(15, 2) NOT NULL,
    order_line_count INT NOT NULL,
    total_quantity INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key),
    FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key),
    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
    FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key),
    FOREIGN KEY (shipping_method_key) REFERENCES dim_shipping_method(shipping_method_key),
    FOREIGN KEY (currency_key) REFERENCES dim_currency(currency_key)
) PARTITION BY RANGE (order_date_key) (
    PARTITION p2020 VALUES LESS THAN (20210101),
    PARTITION p2021 VALUES LESS THAN (20220101),
    PARTITION p2022 VALUES LESS THAN (20230101),
    PARTITION p2023 VALUES LESS THAN (20240101),
    PARTITION p2024 VALUES LESS THAN (20250101),
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create Sales Order Line fact table
CREATE TABLE IF NOT EXISTS fact_sales_order_line (
    sales_order_line_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    sales_order_key BIGINT NOT NULL,
    product_key INT NOT NULL,
    promotion_key INT,
    business_key VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_order_id VARCHAR(50) NOT NULL,
    source_order_line_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(15, 2) NOT NULL,
    unit_cost DECIMAL(15, 2) NOT NULL,
    discount_percent DECIMAL(5, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    line_total_amount DECIMAL(15, 2) NOT NULL,
    gross_profit DECIMAL(15, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key),
    FOREIGN KEY (sales_order_key) REFERENCES fact_sales_order(sales_order_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (promotion_key) REFERENCES dim_promotion(promotion_key)
) PARTITION BY RANGE (sales_order_key) (
    PARTITION p0 VALUES LESS THAN (1000000),
    PARTITION p1 VALUES LESS THAN (2000000),
    PARTITION p2 VALUES LESS THAN (3000000),
    PARTITION p3 VALUES LESS THAN (4000000),
    PARTITION p4 VALUES LESS THAN (5000000),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create Inventory fact table
CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    business_key VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_on_order INT NOT NULL,
    quantity_in_transit INT NOT NULL,
    quantity_reserved INT NOT NULL,
    quantity_available INT GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_point INT,
    reorder_quantity INT,
    unit_cost DECIMAL(15, 2) NOT NULL,
    total_value DECIMAL(15, 2) NOT NULL,
    days_of_supply INT,
    is_stock_out BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
) PARTITION BY RANGE (date_key) (
    PARTITION p2020 VALUES LESS THAN (20210101),
    PARTITION p2021 VALUES LESS THAN (20220101),
    PARTITION p2022 VALUES LESS THAN (20230101),
    PARTITION p2023 VALUES LESS THAN (20240101),
    PARTITION p2024 VALUES LESS THAN (20250101),
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create Campaign Performance fact table
CREATE TABLE IF NOT EXISTS fact_campaign_performance (
    campaign_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    campaign_key INT NOT NULL,
    business_key VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    impressions INT NOT NULL,
    clicks INT NOT NULL,
    unique_visitors INT NOT NULL,
    page_views INT NOT NULL,
    bounce_rate DECIMAL(5, 2) NOT NULL,
    conversion_rate DECIMAL(5, 2) NOT NULL,
    conversions INT NOT NULL,
    cost DECIMAL(15, 2) NOT NULL,
    revenue DECIMAL(15, 2) NOT NULL,
    roi DECIMAL(15, 4) GENERATED ALWAYS AS ((revenue - cost) / NULLIF(cost, 0)) STORED,
    ctr DECIMAL(15, 4) GENERATED ALWAYS AS (clicks / NULLIF(impressions, 0)) STORED,
    cost_per_click DECIMAL(15, 4) GENERATED ALWAYS AS (cost / NULLIF(clicks, 0)) STORED,
    cost_per_acquisition DECIMAL(15, 4) GENERATED ALWAYS AS (cost / NULLIF(conversions, 0)) STORED,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (business_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key)
) PARTITION BY RANGE (date_key) (
    PARTITION p2020 VALUES LESS THAN (20210101),
    PARTITION p2021 VALUES LESS THAN (20220101),
    PARTITION p2022 VALUES LESS THAN (20230101),
    PARTITION p2023 VALUES LESS THAN (20240101),
    PARTITION p2024 VALUES LESS THAN (20250101),
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create indexes for better query performance
CREATE INDEX idx_fact_sales_order_order_date_key ON fact_sales_order(order_date_key);
CREATE INDEX idx_fact_sales_order_customer_key ON fact_sales_order(customer_key);
CREATE INDEX idx_fact_sales_order_store_key ON fact_sales_order(store_key);
CREATE INDEX idx_fact_sales_order_employee_key ON fact_sales_order(employee_key);

CREATE INDEX idx_fact_sales_order_line_sales_order_key ON fact_sales_order_line(sales_order_key);
CREATE INDEX idx_fact_sales_order_line_product_key ON fact_sales_order_line(product_key);
CREATE INDEX idx_fact_sales_order_line_promotion_key ON fact_sales_order_line(promotion_key);

CREATE INDEX idx_fact_inventory_date_key ON fact_inventory(date_key);
CREATE INDEX idx_fact_inventory_product_key ON fact_inventory(product_key);
CREATE INDEX idx_fact_inventory_store_key ON fact_inventory(store_key);

CREATE INDEX idx_fact_campaign_performance_date_key ON fact_campaign_performance(date_key);
CREATE INDEX idx_fact_campaign_performance_campaign_key ON fact_campaign_performance(campaign_key);

