-- Data Warehouse Presentation Layer Tables Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the presentation layer tables (data marts) for the Enterprise Data Warehouse

-- Use the database
USE enterprise_data_warehouse;

-- Sales Data Mart
SET search_path TO sales_mart, public;

-- Create Sales Data Mart tables
-- These are denormalized for reporting and analytics

-- Sales Fact table (aggregated by day, product, store)
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    daily_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    customer_type_key INT NOT NULL,
    employee_key INT,
    promotion_key INT,
    sales_count INT NOT NULL,
    quantity_sold INT NOT NULL,
    gross_sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    net_sales_amount DECIMAL(15, 2) NOT NULL,
    tax_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_margin DECIMAL(5, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (date_key, product_key, store_key, customer_type_key),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES integration.dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES integration.dim_store(store_key)
) PARTITION BY RANGE (date_key) (
    PARTITION p2020 VALUES LESS THAN (20210101),
    PARTITION p2021 VALUES LESS THAN (20220101),
    PARTITION p2022 VALUES LESS THAN (20230101),
    PARTITION p2023 VALUES LESS THAN (20240101),
    PARTITION p2024 VALUES LESS THAN (20250101),
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Sales Performance by Product Category
CREATE TABLE IF NOT EXISTS fact_product_category_sales (
    product_category_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    sales_count INT NOT NULL,
    quantity_sold INT NOT NULL,
    gross_sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    net_sales_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_margin DECIMAL(5, 2) NOT NULL,
    sales_rank INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, product_category),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Sales Performance by Store
CREATE TABLE IF NOT EXISTS fact_store_sales (
    store_sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    store_key INT NOT NULL,
    sales_count INT NOT NULL,
    quantity_sold INT NOT NULL,
    gross_sales_amount DECIMAL(15, 2) NOT NULL,
    discount_amount DECIMAL(15, 2) NOT NULL,
    net_sales_amount DECIMAL(15, 2) NOT NULL,
    cost_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_amount DECIMAL(15, 2) NOT NULL,
    gross_profit_margin DECIMAL(5, 2) NOT NULL,
    sales_per_sqft DECIMAL(15, 2) NOT NULL,
    sales_rank INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, store_key),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (store_key) REFERENCES integration.dim_store(store_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Marketing Data Mart
SET search_path TO marketing_mart, public;

-- Campaign Performance Summary
CREATE TABLE IF NOT EXISTS fact_campaign_performance_summary (
    campaign_performance_summary_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    campaign_key INT NOT NULL,
    channel VARCHAR(50) NOT NULL,
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
    UNIQUE KEY (year_month, campaign_key, channel),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (campaign_key) REFERENCES integration.dim_campaign(campaign_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Customer Segmentation
CREATE TABLE IF NOT EXISTS fact_customer_segmentation (
    customer_segmentation_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    customer_key INT NOT NULL,
    segment VARCHAR(50) NOT NULL,
    recency_days INT NOT NULL,
    frequency INT NOT NULL,
    monetary_value DECIMAL(15, 2) NOT NULL,
    average_order_value DECIMAL(15, 2) NOT NULL,
    lifetime_value DECIMAL(15, 2) NOT NULL,
    first_purchase_date_key INT NOT NULL,
    last_purchase_date_key INT NOT NULL,
    purchase_count INT NOT NULL,
    total_spend DECIMAL(15, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, customer_key),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES integration.dim_customer(customer_key),
    FOREIGN KEY (first_purchase_date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (last_purchase_date_key) REFERENCES integration.dim_date(date_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Finance Data Mart
SET search_path TO finance_mart, public;

-- Financial Performance
CREATE TABLE IF NOT EXISTS fact_financial_performance (
    financial_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    store_key INT,
    region VARCHAR(100),
    revenue DECIMAL(15, 2) NOT NULL,
    cost_of_goods_sold DECIMAL(15, 2) NOT NULL,
    gross_profit DECIMAL(15, 2) NOT NULL,
    operating_expenses DECIMAL(15, 2) NOT NULL,
    marketing_expenses DECIMAL(15, 2) NOT NULL,
    administrative_expenses DECIMAL(15, 2) NOT NULL,
    other_expenses DECIMAL(15, 2) NOT NULL,
    operating_income DECIMAL(15, 2) NOT NULL,
    taxes DECIMAL(15, 2) NOT NULL,
    net_income DECIMAL(15, 2) NOT NULL,
    gross_margin_percent DECIMAL(5, 2) NOT NULL,
    operating_margin_percent DECIMAL(5, 2) NOT NULL,
    net_margin_percent DECIMAL(5, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, IFNULL(store_key, 0)),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (store_key) REFERENCES integration.dim_store(store_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- HR Data Mart
SET search_path TO hr_mart, public;

-- Employee Performance
CREATE TABLE IF NOT EXISTS fact_employee_performance (
    employee_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    employee_key INT NOT NULL,
    store_key INT NOT NULL,
    sales_count INT NOT NULL,
    sales_amount DECIMAL(15, 2) NOT NULL,
    average_transaction_value DECIMAL(15, 2) NOT NULL,
    items_sold INT NOT NULL,
    hours_worked DECIMAL(10, 2) NOT NULL,
    customers_served INT NOT NULL,
    returns_processed INT NOT NULL,
    returns_amount DECIMAL(15, 2) NOT NULL,
    customer_satisfaction_score DECIMAL(3, 2) NOT NULL,
    sales_per_hour DECIMAL(15, 2) NOT NULL,
    items_per_transaction DECIMAL(10, 2) NOT NULL,
    customers_per_hour DECIMAL(10, 2) NOT NULL,
    returns_ratio DECIMAL(5, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, employee_key),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (employee_key) REFERENCES integration.dim_employee(employee_key),
    FOREIGN KEY (store_key) REFERENCES integration.dim_store(store_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Operations Data Mart
SET search_path TO operations_mart, public;

-- Inventory Performance
CREATE TABLE IF NOT EXISTS fact_inventory_performance (
    inventory_performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    year_month INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    average_quantity_on_hand INT NOT NULL,
    average_quantity_available INT NOT NULL,
    end_quantity_on_hand INT NOT NULL,
    end_quantity_available INT NOT NULL,
    stock_out_days INT NOT NULL,
    stock_out_rate DECIMAL(5, 2) NOT NULL,
    inventory_turns DECIMAL(5, 2) NOT NULL,
    days_of_supply INT NOT NULL,
    average_daily_sales DECIMAL(10, 2) NOT NULL,
    inventory_value DECIMAL(15, 2) NOT NULL,
    reorder_point INT NOT NULL,
    safety_stock INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (year_month, product_key, store_key),
    FOREIGN KEY (date_key) REFERENCES integration.dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES integration.dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES integration.dim_store(store_key)
) PARTITION BY RANGE (year_month) (
    PARTITION p2020 VALUES LESS THAN (202101),
    PARTITION p2021 VALUES LESS THAN (202201),
    PARTITION p2022 VALUES LESS THAN (202301),
    PARTITION p2023 VALUES LESS THAN (202401),
    PARTITION p2024 VALUES LESS THAN (202501),
    PARTITION p2025 VALUES LESS THAN (202601),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- Create indexes for better query performance
-- Sales Data Mart
SET search_path TO sales_mart, public;
CREATE INDEX idx_fact_daily_sales_date_key ON fact_daily_sales(date_key);
CREATE INDEX idx_fact_daily_sales_product_key ON fact_daily_sales(product_key);
CREATE INDEX idx_fact_daily_sales_store_key ON fact_daily_sales(store_key);

CREATE INDEX idx_fact_product_category_sales_date_key ON fact_product_category_sales(date_key);
CREATE INDEX idx_fact_product_category_sales_year_month ON fact_product_category_sales(year_month);
CREATE INDEX idx_fact_product_category_sales_category ON fact_product_category_sales(product_category);

CREATE INDEX idx_fact_store_sales_date_key ON fact_store_sales(date_key);
CREATE INDEX idx_fact_store_sales_year_month ON fact_store_sales(year_month);
CREATE INDEX idx_fact_store_sales_store_key ON fact_store_sales(store_key);

-- Marketing Data Mart
SET search_path TO marketing_mart, public;
CREATE INDEX idx_fact_campaign_performance_summary_date_key ON fact_campaign_performance_summary(date_key);
CREATE INDEX idx_fact_campaign_performance_summary_year_month ON fact_campaign_performance_summary(year_month);
CREATE INDEX idx_fact_campaign_performance_summary_campaign_key ON fact_campaign_performance_summary(campaign_key);
CREATE INDEX idx_fact_campaign_performance_summary_channel ON fact_campaign_performance_summary(channel);

CREATE INDEX idx_fact_customer_segmentation_date_key ON fact_customer_segmentation(date_key);
CREATE INDEX idx_fact_customer_segmentation_year_month ON fact_customer_segmentation(year_month);
CREATE INDEX idx_fact_customer_segmentation_customer_key ON fact_customer_segmentation(customer_key);
CREATE INDEX idx_fact_customer_segmentation_segment ON fact_customer_segmentation(segment);

-- Finance Data Mart
SET search_path TO finance_mart, public;
CREATE INDEX idx_fact_financial_performance_date_key ON fact_financial_performance(date_key);
CREATE INDEX idx_fact_financial_performance_year_month ON fact_financial_performance(year_month);
CREATE INDEX idx_fact_financial_performance_store_key ON fact_financial_performance(store_key);
CREATE INDEX idx_fact_financial_performance_region ON fact_financial_performance(region);

-- HR Data Mart
SET search_path TO hr_mart, public;
CREATE INDEX idx_fact_employee_performance_date_key ON fact_employee_performance(date_key);
CREATE INDEX idx_fact_employee_performance_year_month ON fact_employee_performance(year_month);
CREATE INDEX idx_fact_employee_performance_employee_key ON fact_employee_performance(employee_key);
CREATE INDEX idx_fact_employee_performance_store_key ON fact_employee_performance(store_key);

-- Operations Data Mart
SET search_path TO operations_mart, public;
CREATE INDEX idx_fact_inventory_performance_date_key ON fact_inventory_performance(date_key);
CREATE INDEX idx_fact_inventory_performance_year_month ON fact_inventory_performance(year_month);
CREATE INDEX idx_fact_inventory_performance_product_key ON fact_inventory_performance(product_key);
CREATE INDEX idx_fact_inventory_performance_store_key ON fact_inventory_performance(store_key);

