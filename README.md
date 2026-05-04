# sportsbar-etl-proj

End-to-end FMCG distribution data pipeline built on Databricks, implementing a medallion lakehouse architecture with idempotent incremental fact processing.

## Overview

This project simulates a production-grade ETL system for a sports nutrition distribution business operating across three Indian cities (Bengaluru, Hyderabad, New Delhi). It ingests raw operational data from an AWS S3 landing zone, applies layered transformations through PySpark and Delta Lake, and surfaces business-ready tables through a Databricks Lakeview BI dashboard.

The objective: build patterns that behave correctly when re-run — not just notebooks that succeed once.

## Architecture

```
          AWS S3 Landing Zone (raw CSVs)
                      │
                      ▼
        ┌─────────────────────────────────┐
        │       Unity Catalog: fmcg       │
        ├─────────────────────────────────┤
        │  Bronze   →  Silver  →  Gold    │
        │  (raw)    (cleansed)  (modeled) │
        └─────────────────────────────────┘
                      │
                      ▼
              Lakeview BI Dashboard
```

- **Bronze** — raw ingestion with metadata (file name, file size, read timestamp). One row per source record, no transformation.
- **Silver** — deduplicated, trimmed, value-mapped, null-resolved. Conformed types and business rules applied.
- **Gold** — dimensional model with conformed keys, ready for analytics. Fact table maintained via MERGE upserts.

## Technology stack

| Layer          | Tool                                            |
|----------------|-------------------------------------------------|
| Compute        | Databricks                                      |
| Storage        | AWS S3 + Delta Lake                             |
| Governance     | Unity Catalog (`fmcg.bronze`, `silver`, `gold`) |
| Processing     | PySpark, Delta Tables API, Spark SQL            |
| Orchestration  | `dbutils.notebook.run()` chained workflows      |
| Visualization  | Databricks Lakeview Dashboard                   |

## Repository structure

```
sportsbar-etl-proj/
├── 1_setup/
│   ├── setup catalog.ipynb              Unity Catalog and schema creation
│   ├── dim_date_table_creation.ipynb    Date dimension generator
│   └── utilities.ipynb                  Shared variables (schema names, etc.)
│
├── 2_dim_data_process/
│   ├── bronze/                          Raw ingestion for each dimension
│   │   ├── bronze_customer_data_processing.ipynb
│   │   ├── bronze_product_data_processing.ipynb
│   │   └── bronze_pricing_data_processing.ipynb
│   ├── silver/                          Cleansed and validated dimensions
│   │   ├── silver_customer_data_processing.ipynb
│   │   ├── silver_products_data_processing.ipynb
│   │   └── silver_pricing_data_processing.ipynb
│   └── gold/                            Business-ready dimensional model
│       ├── gold_customer_data_processing.ipynb
│       ├── gold_products_data_processing.ipynb
│       └── gold_pricing_data_processing.ipynb
│
├── 3_fact_data_processing/
│   ├── full_load_fact.ipynb             Initial fact table load
│   └── incremental_load_fact.ipynb      Monthly recalc + MERGE upserts
│
├── 4_orchestration/
│   ├── customer_orchestration.ipynb     Chains bronze → silver → gold for customers
│   ├── products_orchestration.ipynb     Same pattern for products
│   └── pricing_orchestration.ipynb      Same pattern for pricing
│
└── 5_dashboard & data analysis/
    └── FMCG Atlikon BI Dashboard.lvdash.json
```

## Pipeline flow

1. Raw CSVs land in `s3://sporsbar-proj-lopez/{data_source}/landing/`.
2. Bronze notebooks ingest into `fmcg.bronze.{table}` and move source files to `processed/`.
3. Silver notebooks read from bronze, deduplicate, cleanse, and write to `fmcg.silver.{table}`.
4. Gold notebooks model dimensions and facts into `fmcg.gold.{table}`.
5. Lakeview dashboard reads exclusively from gold for BI consumption.

## Key design decisions

### Idempotent incremental fact load
The incremental fact load uses a monthly partition recalc strategy: extract the distinct months present in the staging batch, recompute aggregates for those months only, then MERGE into the gold fact on `(date, product_code, customer_code)`. Re-running the same batch produces identical results, and late-arriving data is handled cleanly.

### Landing → processed file movement
After bronze ingestion completes, files in the landing zone are moved to a `processed/` folder. This prevents accidental re-ingestion on subsequent runs and creates an audit trail of what was loaded and when.

### Schema evolution and Change Data Feed
All Delta writes use `mergeSchema = true` and `delta.enableChangeDataFeed = true`. The pipeline handles new columns gracefully and exposes change events for any downstream CDC consumer.

### Data quality in silver
The silver layer handles real-world messiness: duplicate keys are dropped, customer names are trimmed and title-cased, inconsistent city spellings (`Bengaluruu`, `Bengalore`, `NewDheli`) are mapped back to canonical values, and unresolvable nulls are coalesced through business-confirmed overrides rather than synthetic defaults.

### Parameterized, decoupled notebooks
Every transformation notebook uses `dbutils.widgets` for `catalog` and `data_source`, making notebooks reusable across environments and entities. Schema names are sourced from a shared `utilities` notebook via `%run`.

### Notebook orchestration per domain
Each domain (customers, products, pricing) has its own orchestration notebook that chains its bronze → silver → gold notebooks through `dbutils.notebook.run()`. This isolates failure scope and keeps the dependency graph readable.

## Future improvements

- Replace `dbutils.notebook.run()` orchestration with Databricks Workflows or Airflow for proper DAG visualization, retries, and SLA monitoring
- Add explicit data quality checks at each layer (Great Expectations or Soda)
- Add unit tests for transformation logic using `pytest` + `chispa`
- Implement SCD Type 2 on the customer and product dimensions
- Log every pipeline run to a Delta audit table (run_id, layer, table, rows_in, rows_out, duration, status)
- Migrate to Delta Live Tables for declarative pipeline definition

## About

Portfolio project by **Jat Lopez** — BS Information Systems student at the University of Santo Tomas, focused on data engineering, data analytics, and BI.

Repo: [github.com/jatjatlopez/sportsbar-etl-proj](https://github.com/jatjatlopez/sportsbar-etl-proj)
