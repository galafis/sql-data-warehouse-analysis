# SQL Data Warehouse Analysis

<p align="center">
  <img src="images/hero_image.png" alt="SQL Data Warehouse Analysis" width="800"/>
</p>

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB.svg)](https://www.python.org/)
[![SQLite](https://img.shields.io/badge/SQLite-3-003B57.svg)](https://sqlite.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?logo=docker)](Dockerfile)

Toolkit completo para construcao e analise de Data Warehouses utilizando SQL e Python com SQLite. Inclui construcao de star schema, gerenciamento de dimensoes SCD Type 2, pipelines ETL, simulacao de cubos OLAP e analise de otimizacao de queries.

Full toolkit for building and analyzing Data Warehouses using SQL and Python with SQLite. Includes star schema construction, SCD Type 2 dimension management, ETL pipelines, OLAP cube simulation, and query optimization analysis.

---

## Arquitetura / Architecture

```mermaid
graph TB
    subgraph Sources["Fontes de Dados / Data Sources"]
        S1[Transactional DB]
        S2[CSV Files]
        S3[API Feeds]
    end

    subgraph ETL["Pipeline ETL"]
        E1[Extract] --> E2[Transform]
        E2 --> E3[Load]
        E3 --> E4[Validate]
    end

    subgraph DW["Data Warehouse"]
        subgraph Staging["Staging Layer"]
            ST1[Raw Tables]
        end
        subgraph Integration["Integration Layer"]
            I1[dim_customer SCD2]
            I2[dim_product]
            I3[dim_date]
            I4[dim_store]
        end
        subgraph Presentation["Presentation Layer"]
            F1[fact_sales]
            F2[fact_orders]
        end
    end

    subgraph Analytics["Analise / Analytics"]
        A1[OLAP Cube Simulator]
        A2[Query Optimizer]
        A3[Reports & Views]
    end

    S1 --> E1
    S2 --> E1
    S3 --> E1
    E4 --> ST1
    ST1 --> I1
    ST1 --> I2
    ST1 --> I3
    ST1 --> I4
    I1 --> F1
    I2 --> F1
    I3 --> F1
    I4 --> F2
    F1 --> A1
    F1 --> A2
    F2 --> A3
```

## Componentes / Components

```mermaid
classDiagram
    class StarSchemaBuilder {
        +create_dimension_table()
        +create_fact_table()
        +insert_dimension_row()
        +insert_fact_row()
        +get_schema_info()
    }
    class SCDType2Manager {
        +create_scd2_table()
        +upsert()
        +get_history()
    }
    class ETLPipeline {
        +add_step()
        +run()
        +get_log()
    }
    class OLAPCubeSimulator {
        +rollup()
        +slice_cube()
        +dice_cube()
        +pivot()
    }
    class QueryOptimizerAnalyzer {
        +explain()
        +analyze_table()
        +suggest_indexes()
        +create_index()
    }
    class DateDimensionGenerator {
        +create_date_dimension()
    }
    StarSchemaBuilder --> SCDType2Manager
    StarSchemaBuilder --> ETLPipeline
    ETLPipeline --> OLAPCubeSimulator
    OLAPCubeSimulator --> QueryOptimizerAnalyzer
    StarSchemaBuilder --> DateDimensionGenerator
```

## Funcionalidades / Features

| Funcionalidade / Feature | Descricao / Description |
|---|---|
| Star Schema Builder | Criacao de tabelas dimensao e fato com chaves substitutas / Dimension and fact tables with surrogate keys |
| SCD Type 2 | Controle de historico de mudancas em dimensoes / Historical change tracking in dimensions |
| ETL Pipeline | Orquestracao de etapas extract-transform-load / ETL step orchestration with logging |
| OLAP Cube | Roll-up, slice, dice e pivot sobre dados do warehouse / OLAP operations over warehouse data |
| Query Optimizer | Analise de planos de execucao e sugestao de indices / Execution plan analysis and index suggestions |
| Date Dimension | Geracao automatica de calendario dimensional / Automatic calendar dimension generation |

## Estrutura / Project Structure

```
sql-data-warehouse-analysis/
├── src/
│   ├── ddl/                        # DDL scripts
│   │   ├── 01_create_schema.sql
│   │   ├── 02_create_staging_tables.sql
│   │   ├── 03_create_integration_tables.sql
│   │   └── 04_create_presentation_tables.sql
│   ├── etl/
│   │   └── etl_master.sql
│   ├── procedures/
│   │   ├── sp_generate_date_dimension.sql
│   │   ├── sp_load_dimension_scd2.sql
│   │   ├── sp_load_fact_sales_order.sql
│   │   └── sp_load_fact_sales_order_line.sql
│   ├── views/
│   │   ├── vw_campaign_performance.sql
│   │   ├── vw_inventory_analysis.sql
│   │   └── vw_sales_performance.sql
│   └── warehouse_toolkit.py
├── tests/
│   └── test_warehouse_toolkit.py
└── README.md
```

## Inicio Rapido / Quick Start

```python
from src.warehouse_toolkit import StarSchemaBuilder, SCDType2Manager, OLAPCubeSimulator

# Construir star schema
builder = StarSchemaBuilder(":memory:")
builder.create_dimension_table("dim_product", {"name": "TEXT", "category": "TEXT"})
builder.create_fact_table("fact_sales", {"amount": "REAL"}, {"product_sk": "dim_product"})

pk = builder.insert_dimension_row("dim_product", {"name": "Widget", "category": "Tools"})
builder.insert_fact_row("fact_sales", {"product_sk": pk, "amount": 150.0})

# SCD Type 2
scd = SCDType2Manager(builder.conn, "dim_customer")
scd.create_scd2_table({"customer_id": "TEXT", "name": "TEXT", "city": "TEXT"})
scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "SP"}, "2024-01-01")
scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "RJ"}, "2024-06-01")
history = scd.get_history("customer_id", "C001")  # 2 versions

# OLAP
cube = OLAPCubeSimulator(builder.conn, "fact_sales")
results = cube.rollup(["product_sk"], "amount")
```

## Testes / Tests

```bash
pytest tests/ -v
```

## Tecnologias / Technologies

- Python 3.9+
- SQLite3
- pytest

## Licenca / License

MIT License - veja [LICENSE](LICENSE) para detalhes / see [LICENSE](LICENSE) for details.
