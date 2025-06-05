# 🇧🇷 Análise de Data Warehouse com SQL Avançado | 🇺🇸 Advanced SQL Data Warehouse Analysis

<div align="center">

![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)

**Plataforma completa de análise de data warehouse com SQL avançado e ETL moderno**

[📊 Análises](#-análises-disponíveis) • [🏗️ Arquitetura](#-arquitetura) • [⚡ Setup](#-setup-rápido) • [📈 Dashboards](#-dashboards)

</div>

---

## 🇧🇷 Português

### 📊 Visão Geral

Plataforma abrangente de **análise de data warehouse** desenvolvida com SQL avançado e ferramentas modernas de ETL:

- 🏗️ **Arquitetura Dimensional**: Star schema, snowflake, data vault
- 📊 **SQL Avançado**: CTEs, window functions, análises complexas
- 🔄 **ETL Moderno**: dbt para transformações, Airflow para orquestração
- 📈 **Analytics**: KPIs, métricas de negócio, análises temporais
- 🎯 **Performance**: Otimização de queries, indexação, particionamento
- 📋 **Governança**: Qualidade de dados, linhagem, documentação

### 🎯 Objetivos da Plataforma

- **Centralizar dados** de múltiplas fontes operacionais
- **Padronizar métricas** e definições de negócio
- **Acelerar análises** com estruturas otimizadas
- **Garantir qualidade** e consistência dos dados
- **Facilitar self-service** analytics para usuários

### 🛠️ Stack Tecnológico

#### Database e Storage
- **PostgreSQL**: Database principal do data warehouse
- **Amazon Redshift**: Data warehouse na nuvem (opcional)
- **Snowflake**: Plataforma de dados moderna (opcional)
- **Apache Parquet**: Formato colunar para storage

#### ETL e Transformação
- **dbt (data build tool)**: Transformações SQL como código
- **Apache Airflow**: Orquestração de workflows
- **Pandas**: Processamento de dados em Python
- **SQLAlchemy**: ORM e conexões de banco

#### Qualidade e Governança
- **Great Expectations**: Validação de qualidade de dados
- **Apache Atlas**: Catálogo de dados e linhagem
- **dbt docs**: Documentação automática
- **Soda**: Monitoramento de qualidade

#### Visualização e BI
- **Apache Superset**: Dashboards e visualizações
- **Grafana**: Monitoramento e métricas
- **Jupyter**: Análises exploratórias
- **Plotly**: Gráficos interativos

### 📋 Arquitetura do Data Warehouse

```
sql-data-warehouse-analysis/
├── 📁 sql/                        # Scripts SQL organizados
│   ├── 📁 ddl/                    # Data Definition Language
│   │   ├── 📄 create_schemas.sql  # Criação de schemas
│   │   ├── 📄 create_tables.sql   # Criação de tabelas
│   │   ├── 📄 create_indexes.sql  # Criação de índices
│   │   └── 📄 create_views.sql    # Criação de views
│   ├── 📁 dml/                    # Data Manipulation Language
│   │   ├── 📄 insert_dimensions.sql # Carga de dimensões
│   │   ├── 📄 insert_facts.sql    # Carga de fatos
│   │   └── 📄 update_scd.sql      # Slowly Changing Dimensions
│   ├── 📁 analytics/              # Queries analíticas
│   │   ├── 📄 sales_analysis.sql  # Análise de vendas
│   │   ├── 📄 customer_analysis.sql # Análise de clientes
│   │   ├── 📄 product_analysis.sql # Análise de produtos
│   │   └── 📄 financial_analysis.sql # Análise financeira
│   ├── 📁 procedures/             # Stored procedures
│   │   ├── 📄 sp_load_daily.sql   # Carga diária
│   │   ├── 📄 sp_data_quality.sql # Verificação qualidade
│   │   └── 📄 sp_aggregations.sql # Agregações
│   └── 📁 functions/              # User-defined functions
│       ├── 📄 date_functions.sql  # Funções de data
│       ├── 📄 string_functions.sql # Funções de string
│       └── 📄 business_functions.sql # Funções de negócio
├── 📁 dbt/                        # Projeto dbt
│   ├── 📁 models/                 # Modelos dbt
│   │   ├── 📁 staging/            # Camada staging
│   │   │   ├── 📄 stg_customers.sql # Staging clientes
│   │   │   ├── 📄 stg_orders.sql  # Staging pedidos
│   │   │   └── 📄 stg_products.sql # Staging produtos
│   │   ├── 📁 intermediate/       # Camada intermediária
│   │   │   ├── 📄 int_customer_orders.sql # Pedidos por cliente
│   │   │   └── 📄 int_product_sales.sql # Vendas por produto
│   │   ├── 📁 marts/              # Data marts
│   │   │   ├── 📄 dim_customer.sql # Dimensão cliente
│   │   │   ├── 📄 dim_product.sql # Dimensão produto
│   │   │   ├── 📄 dim_date.sql    # Dimensão data
│   │   │   └── 📄 fact_sales.sql  # Fato vendas
│   │   └── 📁 analytics/          # Modelos analíticos
│   │       ├── 📄 customer_metrics.sql # Métricas cliente
│   │       ├── 📄 product_performance.sql # Performance produto
│   │       └── 📄 sales_summary.sql # Resumo vendas
│   ├── 📁 macros/                 # Macros dbt
│   │   ├── 📄 generate_schema_name.sql # Schema naming
│   │   ├── 📄 test_not_null_where.sql # Testes customizados
│   │   └── 📄 pivot_table.sql     # Macro pivot
│   ├── 📁 tests/                  # Testes de dados
│   │   ├── 📄 assert_positive_values.sql # Valores positivos
│   │   └── 📄 assert_valid_dates.sql # Datas válidas
│   ├── 📄 dbt_project.yml         # Configuração dbt
│   └── 📄 profiles.yml            # Perfis de conexão
├── 📁 airflow/                    # DAGs Airflow
│   ├── 📁 dags/                   # Definição de DAGs
│   │   ├── 📄 daily_etl_dag.py    # ETL diário
│   │   ├── 📄 weekly_aggregation_dag.py # Agregações semanais
│   │   └── 📄 data_quality_dag.py # Verificação qualidade
│   ├── 📁 plugins/                # Plugins customizados
│   │   ├── 📄 custom_operators.py # Operadores customizados
│   │   └── 📄 custom_sensors.py   # Sensores customizados
│   └── 📁 config/                 # Configurações
│       ├── 📄 connections.py      # Conexões
│       └── 📄 variables.py        # Variáveis
├── 📁 python/                     # Scripts Python
│   ├── 📁 etl/                    # Scripts ETL
│   │   ├── 📄 extract_sources.py  # Extração de fontes
│   │   ├── 📄 transform_data.py   # Transformações
│   │   └── 📄 load_warehouse.py   # Carga no DW
│   ├── 📁 analysis/               # Análises Python
│   │   ├── 📄 cohort_analysis.py  # Análise de coorte
│   │   ├── 📄 rfm_analysis.py     # Análise RFM
│   │   └── 📄 time_series_analysis.py # Séries temporais
│   ├── 📁 utils/                  # Utilitários
│   │   ├── 📄 database_utils.py   # Utilitários de banco
│   │   ├── 📄 data_quality.py     # Qualidade de dados
│   │   └── 📄 logging_utils.py    # Sistema de logs
│   └── 📁 tests/                  # Testes Python
│       ├── 📄 test_etl.py         # Testes ETL
│       └── 📄 test_data_quality.py # Testes qualidade
├── 📁 notebooks/                  # Jupyter notebooks
│   ├── 📄 01_data_exploration.ipynb # Exploração de dados
│   ├── 📄 02_dimensional_modeling.ipynb # Modelagem dimensional
│   ├── 📄 03_performance_analysis.ipynb # Análise performance
│   └── 📄 04_business_insights.ipynb # Insights de negócio
├── 📁 dashboards/                 # Dashboards e relatórios
│   ├── 📁 superset/              # Dashboards Superset
│   │   ├── 📄 sales_dashboard.json # Dashboard vendas
│   │   └── 📄 executive_dashboard.json # Dashboard executivo
│   ├── 📁 grafana/               # Dashboards Grafana
│   │   ├── 📄 data_quality_dashboard.json # Qualidade dados
│   │   └── 📄 performance_dashboard.json # Performance
│   └── 📁 reports/               # Relatórios
│       ├── 📄 monthly_report.sql  # Relatório mensal
│       └── 📄 quarterly_report.sql # Relatório trimestral
├── 📁 config/                     # Configurações
│   ├── 📄 database_config.yaml    # Configuração banco
│   ├── 📄 etl_config.yaml         # Configuração ETL
│   └── 📄 monitoring_config.yaml  # Configuração monitoramento
├── 📁 docs/                       # Documentação
│   ├── 📄 data_dictionary.md      # Dicionário de dados
│   ├── 📄 business_rules.md       # Regras de negócio
│   └── 📄 architecture.md         # Arquitetura
├── 📁 docker/                     # Containers Docker
│   ├── 📄 Dockerfile.postgres     # PostgreSQL
│   ├── 📄 Dockerfile.airflow      # Airflow
│   └── 📄 docker-compose.yml      # Orquestração
├── 📄 requirements.txt            # Dependências Python
├── 📄 README.md                   # Este arquivo
├── 📄 LICENSE                     # Licença MIT
└── 📄 .gitignore                 # Arquivos ignorados
```

### 📊 Análises Disponíveis

#### 1. 📈 Análise de Vendas

**Vendas por Período**
```sql
-- Análise de vendas com window functions
WITH sales_by_month AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount) AS monthly_sales,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        AVG(total_amount) AS avg_order_value
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE order_date >= CURRENT_DATE - INTERVAL '24 months'
    GROUP BY DATE_TRUNC('month', order_date)
),
sales_with_growth AS (
    SELECT 
        month,
        monthly_sales,
        order_count,
        unique_customers,
        avg_order_value,
        LAG(monthly_sales) OVER (ORDER BY month) AS prev_month_sales,
        (monthly_sales - LAG(monthly_sales) OVER (ORDER BY month)) / 
        LAG(monthly_sales) OVER (ORDER BY month) * 100 AS growth_rate,
        SUM(monthly_sales) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cumulative_sales
    FROM sales_by_month
)
SELECT 
    month,
    monthly_sales,
    ROUND(growth_rate, 2) AS growth_rate_pct,
    cumulative_sales,
    order_count,
    unique_customers,
    ROUND(avg_order_value, 2) AS avg_order_value
FROM sales_with_growth
ORDER BY month;
```

**Análise de Sazonalidade**
```sql
-- Padrões sazonais de vendas
WITH seasonal_analysis AS (
    SELECT 
        EXTRACT(QUARTER FROM dd.date_actual) AS quarter,
        EXTRACT(MONTH FROM dd.date_actual) AS month,
        EXTRACT(DOW FROM dd.date_actual) AS day_of_week,
        SUM(fs.total_amount) AS total_sales,
        COUNT(fs.order_id) AS order_count,
        AVG(fs.total_amount) AS avg_order_value
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '3 years'
    GROUP BY 
        EXTRACT(QUARTER FROM dd.date_actual),
        EXTRACT(MONTH FROM dd.date_actual),
        EXTRACT(DOW FROM dd.date_actual)
)
SELECT 
    quarter,
    month,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    ROUND(AVG(total_sales), 2) AS avg_daily_sales,
    ROUND(AVG(order_count), 0) AS avg_daily_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM seasonal_analysis
GROUP BY quarter, month, day_of_week
ORDER BY quarter, month, day_of_week;
```

#### 2. 👥 Análise de Clientes

**Segmentação RFM**
```sql
-- Análise RFM (Recency, Frequency, Monetary)
WITH customer_rfm AS (
    SELECT 
        dc.customer_id,
        dc.customer_name,
        -- Recency: dias desde a última compra
        CURRENT_DATE - MAX(dd.date_actual) AS recency_days,
        -- Frequency: número de pedidos
        COUNT(DISTINCT fs.order_id) AS frequency,
        -- Monetary: valor total gasto
        SUM(fs.total_amount) AS monetary_value
    FROM dim_customer dc
    JOIN fact_sales fs ON dc.customer_key = fs.customer_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY dc.customer_id, dc.customer_name
),
rfm_scores AS (
    SELECT 
        customer_id,
        customer_name,
        recency_days,
        frequency,
        monetary_value,
        -- Scores RFM (1-5, sendo 5 o melhor)
        NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value ASC) AS monetary_score
    FROM customer_rfm
),
rfm_segments AS (
    SELECT 
        *,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
            THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 
            THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 
            THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 
            THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 
            THEN 'Lost Customers'
            ELSE 'Potential Loyalists'
        END AS customer_segment
    FROM rfm_scores
)
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(recency_days), 1) AS avg_recency,
    ROUND(AVG(frequency), 1) AS avg_frequency,
    ROUND(AVG(monetary_value), 2) AS avg_monetary,
    ROUND(SUM(monetary_value), 2) AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS segment_percentage
FROM rfm_segments
GROUP BY customer_segment
ORDER BY total_revenue DESC;
```

**Análise de Coorte**
```sql
-- Análise de coorte de retenção
WITH customer_cohorts AS (
    SELECT 
        dc.customer_id,
        MIN(dd.date_actual) AS first_purchase_date,
        DATE_TRUNC('month', MIN(dd.date_actual)) AS cohort_month
    FROM dim_customer dc
    JOIN fact_sales fs ON dc.customer_key = fs.customer_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dc.customer_id
),
customer_activities AS (
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', dd.date_actual) AS activity_month,
        EXTRACT(EPOCH FROM (DATE_TRUNC('month', dd.date_actual) - cc.cohort_month)) / 
        EXTRACT(EPOCH FROM INTERVAL '1 month') AS period_number
    FROM customer_cohorts cc
    JOIN fact_sales fs ON cc.customer_id = fs.customer_id
    JOIN dim_date dd ON fs.date_key = dd.date_key
),
cohort_table AS (
    SELECT 
        cohort_month,
        period_number,
        COUNT(DISTINCT customer_id) AS customers
    FROM customer_activities
    GROUP BY cohort_month, period_number
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    ct.cohort_month,
    cs.cohort_size,
    ct.period_number,
    ct.customers,
    ROUND(100.0 * ct.customers / cs.cohort_size, 2) AS retention_rate
FROM cohort_table ct
JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
WHERE ct.period_number <= 12  -- Primeiros 12 meses
ORDER BY ct.cohort_month, ct.period_number;
```

#### 3. 📦 Análise de Produtos

**Performance de Produtos**
```sql
-- Análise de performance de produtos com ranking
WITH product_performance AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        SUM(fs.quantity) AS total_quantity_sold,
        SUM(fs.total_amount) AS total_revenue,
        COUNT(DISTINCT fs.order_id) AS order_count,
        COUNT(DISTINCT fs.customer_id) AS unique_customers,
        AVG(fs.unit_price) AS avg_unit_price,
        SUM(fs.total_amount) / SUM(fs.quantity) AS avg_selling_price
    FROM dim_product dp
    JOIN fact_sales fs ON dp.product_key = fs.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY dp.product_id, dp.product_name, dp.category, dp.subcategory
),
product_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS quantity_rank,
        ROW_NUMBER() OVER (ORDER BY unique_customers DESC) AS customer_reach_rank,
        PERCENT_RANK() OVER (ORDER BY total_revenue) AS revenue_percentile
    FROM product_performance
)
SELECT 
    product_name,
    category,
    subcategory,
    total_quantity_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    order_count,
    unique_customers,
    ROUND(avg_selling_price, 2) AS avg_selling_price,
    revenue_rank,
    quantity_rank,
    customer_reach_rank,
    ROUND(revenue_percentile * 100, 2) AS revenue_percentile
FROM product_rankings
WHERE revenue_rank <= 50  -- Top 50 produtos
ORDER BY revenue_rank;
```

#### 4. 💰 Análise Financeira

**KPIs Financeiros**
```sql
-- Dashboard de KPIs financeiros
WITH financial_metrics AS (
    SELECT 
        DATE_TRUNC('month', dd.date_actual) AS month,
        SUM(fs.total_amount) AS gross_revenue,
        SUM(fs.quantity * dp.cost_price) AS total_cost,
        SUM(fs.total_amount) - SUM(fs.quantity * dp.cost_price) AS gross_profit,
        COUNT(DISTINCT fs.order_id) AS total_orders,
        COUNT(DISTINCT fs.customer_id) AS unique_customers,
        SUM(fs.quantity) AS total_units_sold
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '24 months'
    GROUP BY DATE_TRUNC('month', dd.date_actual)
),
metrics_with_calculations AS (
    SELECT 
        month,
        gross_revenue,
        total_cost,
        gross_profit,
        ROUND(100.0 * gross_profit / gross_revenue, 2) AS gross_margin_pct,
        total_orders,
        unique_customers,
        total_units_sold,
        ROUND(gross_revenue / total_orders, 2) AS avg_order_value,
        ROUND(gross_revenue / unique_customers, 2) AS avg_customer_value,
        ROUND(total_orders::DECIMAL / unique_customers, 2) AS avg_orders_per_customer
    FROM financial_metrics
)
SELECT 
    month,
    gross_revenue,
    total_cost,
    gross_profit,
    gross_margin_pct,
    total_orders,
    unique_customers,
    total_units_sold,
    avg_order_value,
    avg_customer_value,
    avg_orders_per_customer,
    -- Comparação com mês anterior
    LAG(gross_revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(100.0 * (gross_revenue - LAG(gross_revenue) OVER (ORDER BY month)) / 
          LAG(gross_revenue) OVER (ORDER BY month), 2) AS revenue_growth_pct
FROM metrics_with_calculations
ORDER BY month DESC;
```

### 🔄 ETL com dbt

#### Modelo Staging
```sql
-- models/staging/stg_orders.sql
{{ config(materialized='view') }}

WITH source_orders AS (
    SELECT * FROM {{ source('ecommerce', 'orders') }}
),

cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date::DATE AS order_date,
        CASE 
            WHEN status IN ('completed', 'shipped', 'delivered') THEN 'completed'
            WHEN status IN ('cancelled', 'refunded') THEN 'cancelled'
            ELSE 'pending'
        END AS order_status,
        total_amount::DECIMAL(10,2) AS total_amount,
        shipping_cost::DECIMAL(10,2) AS shipping_cost,
        tax_amount::DECIMAL(10,2) AS tax_amount,
        discount_amount::DECIMAL(10,2) AS discount_amount,
        created_at,
        updated_at
    FROM source_orders
    WHERE order_date IS NOT NULL
      AND total_amount > 0
)

SELECT * FROM cleaned_orders
```

#### Modelo Dimensional
```sql
-- models/marts/dim_customer.sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['customer_id'], 'unique': True},
      {'columns': ['email'], 'unique': True}
    ]
) }}

WITH customer_base AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        AVG(total_amount) AS avg_order_value
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'completed'
    GROUP BY customer_id
),

final AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['cb.customer_id']) }} AS customer_key,
        cb.customer_id,
        cb.first_name,
        cb.last_name,
        cb.email,
        cb.phone,
        cb.address,
        cb.city,
        cb.state,
        cb.country,
        cb.postal_code,
        cb.registration_date,
        COALESCE(cm.first_order_date, cb.registration_date) AS first_order_date,
        cm.last_order_date,
        COALESCE(cm.total_orders, 0) AS total_orders,
        COALESCE(cm.lifetime_value, 0) AS lifetime_value,
        COALESCE(cm.avg_order_value, 0) AS avg_order_value,
        CASE 
            WHEN cm.total_orders >= 10 THEN 'VIP'
            WHEN cm.total_orders >= 5 THEN 'Regular'
            WHEN cm.total_orders >= 1 THEN 'New'
            ELSE 'Prospect'
        END AS customer_tier,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM customer_base cb
    LEFT JOIN customer_metrics cm ON cb.customer_id = cm.customer_id
)

SELECT * FROM final
```

### 🎯 Competências Demonstradas

#### SQL Avançado
- ✅ **Window Functions**: ROW_NUMBER, RANK, LAG/LEAD, NTILE
- ✅ **CTEs**: Common Table Expressions complexas
- ✅ **Agregações**: GROUP BY, HAVING, ROLLUP, CUBE
- ✅ **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS

#### Data Warehousing
- ✅ **Modelagem Dimensional**: Star schema, snowflake
- ✅ **ETL/ELT**: Extração, transformação, carga
- ✅ **SCD**: Slowly Changing Dimensions
- ✅ **Particionamento**: Otimização de performance

#### Ferramentas Modernas
- ✅ **dbt**: Transformações como código
- ✅ **Airflow**: Orquestração de workflows
- ✅ **Great Expectations**: Qualidade de dados
- ✅ **Apache Superset**: Visualização e BI

### 🚀 Setup Rápido

#### Docker Compose
```bash
# Clonar repositório
git clone https://github.com/galafis/sql-data-warehouse-analysis.git
cd sql-data-warehouse-analysis

# Iniciar ambiente completo
docker-compose up -d

# Serviços disponíveis:
# - PostgreSQL: localhost:5432
# - Airflow: http://localhost:8080
# - Superset: http://localhost:8088
# - Jupyter: http://localhost:8888
```

#### Configuração dbt
```bash
# Instalar dbt
pip install dbt-postgres

# Configurar perfil
dbt init warehouse_analytics

# Executar transformações
dbt run

# Gerar documentação
dbt docs generate
dbt docs serve
```

### 📈 Casos de Uso Práticos

#### 1. E-commerce Analytics
- Análise de funil de vendas
- Segmentação de clientes
- Análise de coorte de retenção
- Performance de produtos

#### 2. Financial Services
- Análise de risco de crédito
- Detecção de fraude
- Análise de rentabilidade
- Compliance regulatório

#### 3. Healthcare Analytics
- Análise de desfechos clínicos
- Otimização de recursos
- Análise epidemiológica
- Qualidade de atendimento

#### 4. Supply Chain
- Análise de demanda
- Otimização de estoque
- Performance de fornecedores
- Análise de custos logísticos

---

## 🇺🇸 English

### 📊 Overview

Comprehensive **data warehouse analysis platform** developed with advanced SQL and modern ETL tools:

- 🏗️ **Dimensional Architecture**: Star schema, snowflake, data vault
- 📊 **Advanced SQL**: CTEs, window functions, complex analyses
- 🔄 **Modern ETL**: dbt for transformations, Airflow for orchestration
- 📈 **Analytics**: KPIs, business metrics, temporal analyses
- 🎯 **Performance**: Query optimization, indexing, partitioning
- 📋 **Governance**: Data quality, lineage, documentation

### 🎯 Platform Objectives

- **Centralize data** from multiple operational sources
- **Standardize metrics** and business definitions
- **Accelerate analyses** with optimized structures
- **Ensure quality** and data consistency
- **Facilitate self-service** analytics for users

### 📊 Available Analyses

#### 1. 📈 Sales Analysis
- Sales by period with growth rates
- Seasonality patterns
- Product performance ranking
- Customer segmentation

#### 2. 👥 Customer Analysis
- RFM segmentation (Recency, Frequency, Monetary)
- Cohort retention analysis
- Customer lifetime value
- Churn prediction

#### 3. 📦 Product Analysis
- Product performance metrics
- Category analysis
- Inventory optimization
- Price elasticity

#### 4. 💰 Financial Analysis
- Financial KPIs dashboard
- Profitability analysis
- Cost analysis
- Revenue forecasting

### 🎯 Skills Demonstrated

#### Advanced SQL
- ✅ **Window Functions**: ROW_NUMBER, RANK, LAG/LEAD, NTILE
- ✅ **CTEs**: Complex Common Table Expressions
- ✅ **Aggregations**: GROUP BY, HAVING, ROLLUP, CUBE
- ✅ **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS

#### Data Warehousing
- ✅ **Dimensional Modeling**: Star schema, snowflake
- ✅ **ETL/ELT**: Extract, transform, load
- ✅ **SCD**: Slowly Changing Dimensions
- ✅ **Partitioning**: Performance optimization

#### Modern Tools
- ✅ **dbt**: Transformations as code
- ✅ **Airflow**: Workflow orchestration
- ✅ **Great Expectations**: Data quality
- ✅ **Apache Superset**: Visualization and BI

---

## 📄 Licença | License

MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes | see [LICENSE](LICENSE) file for details

## 📞 Contato | Contact

**GitHub**: [@galafis](https://github.com/galafis)  
**LinkedIn**: [Gabriel Demetrios Lafis](https://linkedin.com/in/galafis)  
**Email**: gabriel.lafis@example.com

---

<div align="center">

**Desenvolvido com ❤️ para Analytics Empresarial | Developed with ❤️ for Enterprise Analytics**

[![GitHub](https://img.shields.io/badge/GitHub-galafis-blue?style=flat-square&logo=github)](https://github.com/galafis)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

</div>

