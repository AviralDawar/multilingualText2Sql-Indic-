# IndicDB - Indian Databases for Text-to-SQL Benchmarking

A collection of normalized Indian datasets designed for testing complex SQL reasoning (JOINs, Aggregations, Subqueries) in Text-to-SQL benchmarks.

**Supports both Snowflake and PostgreSQL backends.**

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Quick Start with Docker (PostgreSQL)](#quick-start-with-docker-postgresql)
5. [Pipeline: Creating a New Database](#pipeline-creating-a-new-database)
   - [Step 1: Download and Prepare Data](#step-1-download-and-prepare-data)
   - [Step 2: Generate Schema Design (LLM)](#step-2-generate-schema-design-llm)
   - [Step 3: Parse Schema to YAML Config](#step-3-parse-schema-to-yaml-config)
   - [Step 4: Run Generic Data Splitter](#step-4-run-generic-data-splitter)
   - [Step 5: Create Tables](#step-5-create-tables)
   - [Step 6: Load Data](#step-6-load-data)
6. [Troubleshooting](#troubleshooting)
7. [Available Databases](#available-databases)

---

## Overview

This project provides a standardized pipeline for:
- Converting single-table CSV datasets into normalized multi-table databases
- Creating database schemas with proper foreign key relationships
- Loading data for Text-to-SQL benchmark evaluation
- **Supporting both Snowflake and PostgreSQL backends**

---

## Project Structure

```
IndicDB/
├── config/
│   ├── snowflake_credential.json      # Snowflake connection config
│   ├── postgres_credential.json       # PostgreSQL connection config
│   ├── postgres_config.template.json  # PostgreSQL config template
│   └── docker-compose.yml             # Docker setup for PostgreSQL
├── databases/
│   ├── INDIA_POPULATION_CENSUS/
│   │   └── INDIA_POPULATION_CENSUS/
│   │       ├── data/                  # CSV files for each table
│   │       ├── *.json                 # Sample rows for each table
│   │       ├── schema_info.md         # LLM-generated schema design
│   │       ├── schema_config.yaml     # Machine-readable schema config
│   │       ├── DDL.csv                # Snowflake table definitions
│   │       ├── DDL_postgres.csv       # PostgreSQL table definitions
│   │       └── README.md              # Schema documentation
│   └── ...
├── gold/
│   └── sql/                           # Benchmark SQL queries
├── scripts/
│   ├── create_schema_llm_judge.py     # LLM-based schema designer (3-agent pattern)
│   ├── parse_schema_to_yaml.py        # Parse schema_info.md → schema_config.yaml
│   ├── generic_split.py               # Config-driven data splitter
│   ├── db_utils.py                    # Shared database utilities
│   ├── create_tables.py               # Create tables (Snowflake/PostgreSQL)
│   ├── load_data.py                   # Load CSV data
│   ├── run_query.py                   # Run ad-hoc queries
│   ├── execute_queries.py             # Execute benchmark queries
│   ├── validate_queries.py            # Validate SQL queries
│   └── generate_ddl.py                # Auto-generate DDL from CSVs
└── README.md                          # This file
```

---

## Prerequisites

### 1. Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip3 install -r requirements.txt
```

### 2. Database Configuration

#### Option A: PostgreSQL

Create `config/postgres_credential.json`:

```json
{
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "your-password",
    "database": "indicdb"
}
```

---

## Pipeline: Creating a New Database

The pipeline converts a raw CSV into a normalized multi-table database in 6 steps:

```
CSV → LLM Schema Design → YAML Config → Split Data → Create Tables → Load Data
```

### Step 1: Download and Prepare Data

1. Download the CSV file from [NDAP](https://ndap.niti.gov.in/) or other data sources
2. Rename it to `total_data.csv`
3. Create the folder structure:

```bash
mkdir -p databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/data
mv your_downloaded_file.csv databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/data/total_data.csv
```

---

### Step 2: Generate Schema Design (LLM)

Use the LLM-based schema designer to create a normalized schema:

```bash
cd scripts

python create_schema_llm_judge.py \
  ../databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/data/total_data.csv
```

**What it does:**
- Uses a 3-agent pattern (Architect → Auditor → Refiner)
- Architect drafts a star schema with 4-10 thematic tables
- Auditor checks for normalization issues and "God Tables"
- Refiner produces final schema with column mappings

**Output:** `schema_info.md` in the database folder containing:
- Dimension and Fact table definitions
- Column mappings from CSV indices to table columns
- Foreign key relationships

---

### Step 3: Parse Schema to YAML Config

Convert the LLM-generated markdown to a machine-readable YAML config:

```bash
python parse_schema_to_yaml.py \
  ../databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/schema_info.md
```

**Output:** `schema_config.yaml` with structured table definitions:

```yaml
database_name: YOUR_DATABASE_NAME
dimension_tables:
  DIM_GEOGRAPHY:
    key_column: GEOGRAPHY_ID
    dedup_columns: [0, 1, 2]
    columns:
      - source_index: 0
        target_name: COUNTRY
      - source_index: 1
        target_name: STATE
fact_tables:
  FACT_METRICS:
    key_column: METRICS_ID
    foreign_keys:
      - column: GEOGRAPHY_ID
        references: DIM_GEOGRAPHY
    columns:
      - source_index: 10
        target_name: POPULATION
```

---

### Step 4: Run Generic Data Splitter

Split the raw CSV into normalized tables using the YAML config:

```bash
python generic_split.py \
  --config ../databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/schema_config.yaml \
  --input ../databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME/data/total_data.csv \
  --output ../databases/YOUR_DATABASE_NAME/YOUR_DATABASE_NAME
```

**Output Generated:**

| File Type | Location | Purpose |
|-----------|----------|---------|
| `*.csv` | `data/` folder | Full table data for loading |
| `*.json` | Schema folder | Sample rows for Text-to-SQL context |
| `DDL.csv` | Schema folder | CREATE TABLE statements |

---

### Step 5: Create Tables

#### PostgreSQL

```bash
python create_tables.py \
  --backend postgres \
  --database YOUR_DATABASE_NAME \
  --use-database indicdb \
  --use-schema your_schema \
  --create-schema
```

#### Snowflake

```bash
python create_tables.py \
  --backend snowflake \
  --database YOUR_DATABASE_NAME \
  --create-database \
  --create-schema
```

**Flags:**
- `--backend` : Database backend (`postgres` or `snowflake`)
- `--database` : Source database folder name
- `--use-database` : Target database name
- `--use-schema` : Target schema name
- `--create-schema` : Create the schema if it doesn't exist
- `--dry-run` : Preview DDL without executing

---

### Step 6: Load Data

#### PostgreSQL

```bash
python load_data.py \
  --backend postgres \
  --database YOUR_DATABASE_NAME \
  --use-database indicdb \
  --use-schema your_schema \
  --limit 20000
```

#### Snowflake

```bash
python load_data.py \
  --backend snowflake \
  --database YOUR_DATABASE_NAME \
  --limit 20000
```

**Flags:**
- `--limit` : Number of rows to load (optional, for testing)
- `--truncate` : Truncate tables before loading
- `--table` : Load only a specific table

---

## Troubleshooting

### Error: String Too Long

**Error Message:**
```
String 'Very Long Village Name...' is too long and would be truncated
```

**Solution:** Increase VARCHAR column size

```bash
# PostgreSQL
python run_query.py --backend postgres \
  --use-database indicdb --use-schema public \
  --query "ALTER TABLE LOCATIONS ALTER COLUMN VILLAGE_TOWN TYPE VARCHAR(200)"
```

---

### Error: Foreign Key Constraint

If loading fails due to foreign key order, load dimension tables first:

```bash
# Load in order: Dimensions first, then Fact tables
python load_data.py --backend postgres --database YOUR_DB --table DIM_GEOGRAPHY
python load_data.py --backend postgres --database YOUR_DB --table DIM_TIME
python load_data.py --backend postgres --database YOUR_DB --table FACT_METRICS
```

---

## Available Databases

### 1. INDIA_POPULATION_CENSUS

| Metric | Value |
|--------|-------|
| Tables | 7 |
| Total Columns | 78 |
| Foreign Keys | 6 |
| Source | Census of India |

**Tables:** LOCATIONS, CENSUS_POPULATION, CENSUS_CASTE, CENSUS_LITERACY, CENSUS_WORKERS_SUMMARY, CENSUS_MAIN_WORKERS_DETAIL, CENSUS_MARGINAL_WORKERS_DETAIL

---

### 2. INDIA_EMPLOYMENT_DATA

| Metric | Value |
|--------|-------|
| Tables | 7 |
| Total Columns | 78 |
| Foreign Keys | 6 |
| Source | NDAP Economic Census |

**Tables:** LOCATIONS, TIME_PERIOD, EMPLOYMENT_FACT, EMPLOYMENT_BY_INDUSTRY, EMPLOYMENT_BY_OWNERSHIP, EMPLOYMENT_BY_DEMOGRAPHICS, EMPLOYMENT_BY_FIRM_CHARACTERISTICS

---

### 3. INDIA_VILLAGE_AMENITIES

| Metric | Value |
|--------|-------|
| Tables | 16 |
| Total Columns | 50 |
| Source | Census Village Amenities |

**Dimension Tables:** DIM_COUNTRY, DIM_STATE, DIM_DISTRICT, DIM_GEOGRAPHY

**Fact Tables:** FACT_VILLAGE_DEMOGRAPHICS, FACT_TRANSPORTATION_ACCESSIBILITY, FACT_ROAD_HIERARCHY_ACCESSIBILITY, FACT_ROAD_SURFACE_ACCESSIBILITY, FACT_ALTERNATIVE_TRANSPORT_ACCESSIBILITY, FACT_FINANCIAL_SERVICES_ACCESSIBILITY, FACT_MARKET_ACCESSIBILITY, FACT_HEALTH_NUTRITION_ACCESSIBILITY, FACT_EDUCATION_INFORMATION_ACCESSIBILITY, FACT_RECREATION_CULTURE_ACCESSIBILITY, FACT_CIVIC_SERVICES_ACCESSIBILITY

---

### 4. INDIA_SCHOOL_INFRASTRUCTURE

| Metric | Value |
|--------|-------|
| Tables | 15 |
| Total Columns | 55 |
| Source | UDISE School Data |

**Dimension Tables:** DIM_COUNTRY, DIM_STATE, DIM_DISTRICT, DIM_ADMINISTRATIVE_DIVISIONS, DIM_SCHOOL_IDENTITY, DIM_SCHOOL_LOCATION, DIM_SCHOOL_INFRASTRUCTURE, DIM_ACADEMIC_PROGRAMS, DIM_PEDAGOGY, DIM_EVALUATION_METHODOLOGY, DIM_RECOGNITION_YEARS

**Fact Tables:** FACT_INSTRUCTIONAL_CALENDAR, FACT_SCHOOL_HOURS, FACT_TEACHER_WORKING_HOURS, FACT_ANGANWADI_OPERATIONS

---

## Useful Commands

### Interactive Query Mode

```bash
# PostgreSQL
python scripts/run_query.py --backend postgres --use-database indicdb --interactive

# Snowflake
python scripts/run_query.py --backend snowflake --interactive
```

### Execute Benchmark Queries

```bash
# PostgreSQL
python scripts/execute_queries.py --backend postgres --database indicdb --schema public

# Snowflake
python scripts/execute_queries.py --backend snowflake --database INDIA_POPULATION_CENSUS
```

### Validate Queries

```bash
# PostgreSQL
python scripts/validate_queries.py --backend postgres --queries-dir ../gold/sql \
  --database indicdb --schema public
```

---

## License

This project is for research and educational purposes.
