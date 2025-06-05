-- Data Warehouse Presentation Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the presentation layer tables (data marts) for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;

-- Sales Data Mart
-- Daily Sales fact table
CREATE TABLE IF NOT EXISTS sales_mart.fact_daily_sales (
    daily_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    customer_key BIGINT NOT NULL,
    sales_date DATE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    product_subcategory VARCHAR(100),
    store_name VARCHAR(255) NOT NULL,
    store_city VARCHAR(100) NOT NULL,
    store_state VARCHAR(100) NOT NULL,
    store_country VARCHAR(100) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_country VARCHAR(100),
    order_count INT NOT NULL,
    unit_count INT NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    profit_amount DECIMAL(15, 2) NOT NULL,
    profit_margin DECIMAL(15, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_daily_sales_date (date_key),
    INDEX idx_fact_daily_sales_product (product_key),
    INDEX idx_fact_daily_sales_store (store_key),
    INDEX idx_fact_daily_sales_customer (customer_key),
    INDEX idx_fact_daily_sales_category (product_category)
) PARTITION BY RANGE (date_key) (
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Product Category Sales fact table
CREATE TABLE IF NOT EXISTS sales_mart.fact_product_category_sales (
    product_category_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    product_subcategory VARCHAR(100),
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    order_count INT NOT NULL,
    unit_count INT NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    profit_amount DECIMAL(15, 2) NOT NULL,
    profit_margin DECIMAL(15, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_product_category_sales_date (date_key),
    INDEX idx_fact_product_category_sales_category (product_category),
    INDEX idx_fact_product_category_sales_year_month (year_number, month_number)
);

-- Store Sales fact table
CREATE TABLE IF NOT EXISTS sales_mart.fact_store_sales (
    store_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    store_key BIGINT NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    store_city VARCHAR(100) NOT NULL,
    store_state VARCHAR(100) NOT NULL,
    store_country VARCHAR(100) NOT NULL,
    store_region VARCHAR(100),
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    order_count INT NOT NULL,
    unit_count INT NOT NULL,
    customer_count INT NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    profit_amount DECIMAL(15, 2) NOT NULL,
    profit_margin DECIMAL(15, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_store_sales_date (date_key),
    INDEX idx_fact_store_sales_store (store_key),
    INDEX idx_fact_store_sales_year_month (year_number, month_number)
);

-- Marketing Data Mart
-- Campaign Performance Summary fact table
CREATE TABLE IF NOT EXISTS marketing_mart.fact_campaign_performance_summary (
    campaign_performance_summary_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    campaign_key BIGINT NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    impressions INT NOT NULL,
    clicks INT NOT NULL,
    unique_visitors INT NOT NULL,
    page_views INT NOT NULL,
    bounce_rate DECIMAL(5, 2) NOT NULL,
    conversion_rate DECIMAL(5, 2) NOT NULL,
    conversions INT NOT NULL,
    cost DECIMAL(15, 2) NOT NULL,
    revenue DECIMAL(15, 2) NOT NULL,
    roi DECIMAL(15, 4) NOT NULL,
    ctr DECIMAL(15, 4) NOT NULL,
    cost_per_click DECIMAL(15, 4) NOT NULL,
    cost_per_acquisition DECIMAL(15, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_campaign_performance_summary_date (date_key),
    INDEX idx_fact_campaign_performance_summary_campaign (campaign_key),
    INDEX idx_fact_campaign_performance_summary_year_month (year_number, month_number)
);

-- Customer Segmentation fact table
CREATE TABLE IF NOT EXISTS marketing_mart.fact_customer_segmentation (
    customer_segmentation_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key BIGINT NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_country VARCHAR(100),
    customer_region VARCHAR(100),
    segment VARCHAR(50) NOT NULL,
    recency INT,
    frequency INT,
    monetary DECIMAL(15, 2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    lifetime_value DECIMAL(15, 2),
    average_order_value DECIMAL(15, 2),
    purchase_frequency DECIMAL(10, 4),
    days_since_last_purchase INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_customer_segmentation_date (date_key),
    INDEX idx_fact_customer_segmentation_customer (customer_key),
    INDEX idx_fact_customer_segmentation_segment (segment)
);

-- Finance Data Mart
-- Financial Performance fact table
CREATE TABLE IF NOT EXISTS finance_mart.fact_financial_performance (
    financial_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    store_key BIGINT,
    product_category VARCHAR(100),
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    shipping_amount DECIMAL(15, 2) NOT NULL,
    cost_of_goods_sold DECIMAL(15, 2) NOT NULL,
    gross_profit DECIMAL(15, 2) NOT NULL,
    gross_margin DECIMAL(15, 4) NOT NULL,
    operating_expenses DECIMAL(15, 2) NOT NULL,
    operating_profit DECIMAL(15, 2) NOT NULL,
    operating_margin DECIMAL(15, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_financial_performance_date (date_key),
    INDEX idx_fact_financial_performance_store (store_key),
    INDEX idx_fact_financial_performance_category (product_category),
    INDEX idx_fact_financial_performance_year_month (year_number, month_number)
);

-- HR Data Mart
-- Employee Performance fact table
CREATE TABLE IF NOT EXISTS hr_mart.fact_employee_performance (
    employee_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    employee_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    employee_name VARCHAR(255) NOT NULL,
    job_title VARCHAR(100) NOT NULL,
    department VARCHAR(100) NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    sales_target DECIMAL(15, 2) NOT NULL,
    sales_achievement_percent DECIMAL(15, 4) NOT NULL,
    order_count INT NOT NULL,
    customer_count INT NOT NULL,
    average_order_value DECIMAL(15, 2) NOT NULL,
    units_per_transaction DECIMAL(10, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_employee_performance_date (date_key),
    INDEX idx_fact_employee_performance_employee (employee_key),
    INDEX idx_fact_employee_performance_store (store_key),
    INDEX idx_fact_employee_performance_year_month (year_number, month_number)
);

-- Operations Data Mart
-- Inventory Performance fact table
CREATE TABLE IF NOT EXISTS operations_mart.fact_inventory_performance (
    inventory_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_key BIGINT NOT NULL,
    store_key BIGINT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    product_subcategory VARCHAR(100),
    store_name VARCHAR(255) NOT NULL,
    store_city VARCHAR(100) NOT NULL,
    store_state VARCHAR(100) NOT NULL,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    beginning_inventory INT NOT NULL,
    ending_inventory INT NOT NULL,
    quantity_sold INT NOT NULL,
    quantity_received INT NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_available INT NOT NULL,
    inventory_value DECIMAL(15, 2) NOT NULL,
    inventory_turns DECIMAL(10, 4),
    days_of_supply INT,
    stock_out_days INT,
    stock_out_rate DECIMAL(5, 2),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_fact_inventory_performance_date (date_key),
    INDEX idx_fact_inventory_performance_product (product_key),
    INDEX idx_fact_inventory_performance_store (store_key),
    INDEX idx_fact_inventory_performance_year_month (year_number, month_number)
);

-- Print completion message
SELECT 'Enterprise Data Warehouse presentation tables created successfully.' AS result;

