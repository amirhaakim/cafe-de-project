# Cafe Data Engineering dbt Project

This dbt project implements a production-style warehouse pipeline for cafe sales data, from raw ingestion through curated transformation and analytics-ready serving layers.

It follows a practical analytics engineering architecture: source ingestion into Postgres, medallion-style refinement, controlled correction workflows, dimensional modeling, and report-ready marts for BI consumption.

## Project Purpose

This project is designed to demonstrate:

- Python-based CSV ingestion into Postgres
- medallion-style transformation layers: bronze, silver, curated silver, dimensions, and facts
- dbt model organization and materialization strategy
- data quality handling using tests
- a controlled manual correction workflow for bad or missing source values
- dimensional modeling with fact and dimension tables
- analytics-oriented marts for reporting use cases

## Data Flow

```text
dirty_cafe_sales.csv
  -> data_ingestion.py
  -> public.cafe_sales
  -> bronze
  -> silver
  -> silver_curated
  -> dimensions / facts
  -> marts
```

## Lineage Graph (dbt Docs)

Generate project documentation and lineage metadata:

```bash
dbt docs generate
```

Serve the docs locally:

```bash
dbt docs serve
```

Then open the local docs URL and use the lineage graph to inspect upstream/downstream dependencies across `bronze`, `silver`, `silver_curated`, `dimensions`, `facts`, and `marts`.

Why this helps:

- makes model dependencies explicit and reviewable
- speeds up impact analysis before changing a model
- helps validate that transformation layers are organized as intended
- improves project handover and stakeholder communication

## Data Ingestion

File: `../data_ingestion.py`

The ingestion script loads the local CSV file into Postgres before dbt transformations run.

Input:

- `../dirty_cafe_sales.csv`

Output table:

- `public.cafe_sales`

What the script does:

- reads `dirty_cafe_sales.csv` with pandas
- converts CSV column names to lowercase
- connects to Postgres using `engine_credentials` from `.env`
- replaces `public.cafe_sales` with the loaded CSV data

Run from the repository root:

```bash
cd /Users/amirhakim/Cafe_DE
python data_ingestion.py
```

Expected output:

```text
Loaded <row_count> rows into cafe_db.public.cafe_sales
```

## Main Layers

### Sources

Sources are declared in `models/sources.yml`.

Current sources:

- `cafe.cafe_sales`: raw cafe sales source table
- `ops.silver_corrections`: manual correction table maintained outside dbt

### Bronze

Folder: `models/bronze`

The bronze layer represents the raw source data exposed to dbt with minimal transformation.

Main model:

- `bronze_cafe_sales`

### Silver

Folder: `models/silver`

The silver layer standardizes column names, casts data types, and converts invalid source values such as `ERROR` and `UNKNOWN` into cleaner values.

Main models:

- `silver_cleaned_1`
- `silver_cleaned_2`

### Curated Silver

Folder: `models/silver_curated`

The curated silver layer applies approved human corrections from `ops.silver_corrections` on top of `silver_cleaned_2`.

Main model:

- `silver_curated`

This model keeps the dbt-generated silver layer separate from manual intervention. Humans do not edit dbt output tables directly. Instead, they insert corrections into `ops.silver_corrections`, and dbt applies those corrections during the curated model run.

### Dimensions

Folder: `models/dimensions`

Dimension tables provide descriptive context for facts.

Current dimensions:

- `dim_date`
- `dim_item`
- `dim_location`
- `dim_payment_method`

Each dimension includes a surrogate key and an unknown member using key `-1`.

### Facts

Folder: `models/facts`

The fact table contains the measurable sales transactions and foreign keys to the dimension tables.

Current fact model:

- `fct_table`

The grain of the fact table is one row per transaction.

### Marts

Folder: `models/marts`

Marts provide report-ready aggregates built on top of `facts.fct_table` and dimensions.

Current marts:

- `marts_daily_sales`
- `marts_daily_sales_item`
- `marts_daily_sales_location`
- `marts_daily_sales_payment`

Current marts are focused on sales analysis by:

- day
- item
- location
- payment method

## Manual Correction Workflow

The manual correction table is created directly in Postgres:

```sql
create schema if not exists ops;

create table if not exists ops.silver_corrections (
  transaction_id text primary key,
  item text,
  quantity integer,
  price_per_unit numeric,
  total_spent numeric,
  payment_method text,
  location text,
  transaction_date date,
  corrected_by text not null,
  corrected_at timestamp not null default now(),
  reason text
);
```

Workflow:

1. dbt builds `silver_cleaned_2` from the source data.
2. Humans add approved fixes into `ops.silver_corrections`.
3. dbt builds `silver_curated` by merging the base silver data with correction values.
4. Dimensions and facts are built from `silver_curated`.

## Data Model

The dimensional model contains:

- `dimensions.dim_date`
- `dimensions.dim_item`
- `dimensions.dim_location`
- `dimensions.dim_payment_method`
- `facts.fct_table`
- `marts.marts_daily_sales`
- `marts.marts_daily_sales_item`
- `marts.marts_daily_sales_location`
- `marts.marts_daily_sales_payment`

The fact table stores dimension keys:

- `date_key`
- `item_key`
- `location_key`
- `payment_method_key`

Relationships are validated using dbt `relationships` tests. Physical foreign key constraints are not currently enforced in Postgres.

## Dimensional Model Details

Fact table:

- `facts.fct_table`
- grain: one row per transaction (`transaction_id`)
- measures: `quantity`, `price_per_unit`, `total_spent`
- data quality/context fields: `is_total_reconciled`, `total_spent_source`
- foreign keys: `date_key`, `item_key`, `location_key`, `payment_method_key`

Dimension tables:

- `dimensions.dim_date`
  - one row per calendar date + one unknown member (`date_key = -1`)
  - supports time-based slicing (day, month, quarter, year, weekday)
- `dimensions.dim_item`
  - one row per distinct item + one unknown member (`item_key = -1`)
  - supports product/item-level analysis
- `dimensions.dim_location`
  - one row per distinct location + one unknown member (`location_key = -1`)
  - supports branch/location-level analysis
- `dimensions.dim_payment_method`
  - one row per distinct payment method + one unknown member (`payment_method_key = -1`)
  - supports payment-mix analysis

## Schema Naming

This project overrides dbt's default schema naming behavior using:

- `macros/generate_schema_name.sql`

This allows model folders to build into exact schema names such as:

- `bronze`
- `silver`
- `silver_curated`
- `dimensions`
- `facts`
- `marts`

## Tests and Packages

This project uses:

- built-in dbt generic tests (`unique`, `not_null`, `relationships`)
- `dbt_utils` package for extended testing/macros (installed via `packages.yml`)

Silver model tests are defined for both:

- `silver_cleaned_1.transaction_id` (`unique`, `not_null`)
- `silver_cleaned_2.transaction_id` (`unique`, `not_null`)

Unknown-member flags in dimensions/marts are stored as `Yes`/`No` text (not booleans).

## Running the Project

First, load the source data into Postgres:

```bash
cd /Users/amirhakim/Cafe_DE
python data_ingestion.py
```

Run commands from the dbt project directory:

```bash
cd /Users/amirhakim/Cafe_DE/DE_v1
```

Build all models:

```bash
dbt run
```

Build only one specific model/table:

```bash
dbt run --select fct_table
```

Run tests:

```bash
dbt test
```

Run both models and tests:

```bash
dbt build
```

Build only the dimensional model:

```bash
dbt build --select "dim_date dim_item dim_location dim_payment_method fct_table"
```

Build only marts:

```bash
dbt build --select marts
```

Run tests only for silver models:

```bash
dbt test --select "silver_cleaned_1 silver_cleaned_2"
```

## Notes

- The current active dbt target is `dev`, which points to local Postgres.
- `dirty_cafe_sales.csv` is ignored by git and is not committed to the repository.
- DBeaver ERD lines require physical FK constraints in Postgres. This project currently uses dbt relationship tests instead of physical FK constraints.
