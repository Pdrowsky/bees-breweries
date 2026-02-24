# Breweries Medallion Pipeline (Airflow + Docker)

This project implements a Data Lake pipeline using the Medallion Architecture, orchestrated with Apache Airflow running in Docker.

The pipeline consumes data from the **Open Brewery DB API** and processes it through three layers:

- **Bronze**: raw API ingestion (JSON)
- **Silver**: curated, columnar data (Parquet), partitioned by location
- **Gold**: aggregated analytics-ready data (brewery counts by type and location)

---

## Architecture Overview

**Source → Bronze → Silver → Gold**

1. **Bronze Layer**
   - Fetches raw data from the breweries API
   - Persists data in JSON format (native API payload)
   - Append-only, no business transformations

2. **Silver Layer**
   - Reads raw bronze data
   - Applies data curation:
     - column selection
     - schema normalization
     - deduplication by business key (`id`)
   - Persists data in Parquet format
   - Partitions data by `country` and `state`

3. **Gold Layer**
   - Reads the curated silver dataset
   - Aggregates brewery counts by:
     - `brewery_type`
     - `country`
     - `state`
   - Produces an analytics-ready fact table

Each layer is executed as a separate Airflow task, allowing independent monitoring and retries.

---

## Repository Structure

```
bees-breweries/
├── breweries_pipeline/             # Core pipeline module
│   ├── apis/                       # API client & integrations
│   │   ├── __init__.py
│   │   ├── client.py               # OpenBrew DB API client
│   │   └── config.py               # API configuration
│   ├── datalake/                   # Filesystem abstraction
│   │   ├── __init__.py
│   │   ├── filesystem.py           # Storage operations (read/write)
│   │   └── naming_convention.py    # Path/file naming strategy
│   ├── jobs/                       # ETL job implementations
│   │   ├── __init__.py
│   │   ├── ingest_bronze.py        # Raw data ingestion
│   │   ├── transform_silver.py     # Bronze → Silver transformation
│   │   └── build_gold.py           # Silver → Gold aggregation
│   ├── transform/                  # Transformation logic
│   │   ├── __init__.py
│   │   └── data_manipulation.py    # Column selection, dedup logic
│   ├── quality/                    # Data quality checks
│   │   ├── __init__.py
│   │   └── validations.py          # Schema & contract validations
│   ├── analytics/                  # Analytical queries (future)
│   │   └── __init__.py
│   ├── config.py                   # Global pipeline configuration
│   └── __init__.py
├── dags/                           # Airflow DAGs
│   └── breweries_dag.py            # Main pipeline orchestration
├── config/                         # Airflow configuration
│   └── airflow.cfg                 # Airflow settings
├── storage/                        # Data lake root (mounted volume)
│   ├── bronze/                     # Raw data layer
│   ├── silver/                     # Curated data layer
│   └── gold/                       # Analytics layer
├── Dockerfile                      # Custom Airflow image
├── docker-compose.yaml             # Full stack orchestration
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables
└── README.md                       # This file
```

---

## Configuration

### DataLake Root

All data is written under a single lake root directory.

The pipeline reads the location from the `LAKE_ROOT` environment variable, with a safe default:

```python
LAKE_ROOT = os.getenv("LAKE_ROOT", str(Path.cwd() / "storage"))
```

Which resolves to /opt/airflow/storage inside Docker.

The storage/ folder is mounted as a volume, allowing data to persist outside containers.


---

## Quick Start

### Prerequisites

- **Docker Desktop** installed and running
- **Docker Compose** 
- **Git** (for cloning the repository)
- **Bash** (Git Bash on Windows)

### Step 1: Clone Repository

```bash
git clone https://github.com/Pdrowsky/bees-breweries.git
cd bees-breweries
```

### Step 2: Initialize Airflow

On **first run only**, initialize the Airflow metadata database and create the admin user:

```bash
# Build the custom Airflow image with pipeline dependencies
docker compose build

# Initialize Airflow
docker compose up airflow-init
```

**Credentials**:
- Username: `airflow`
- Password: `airflow`

### Step 3: Start the Stack

```bash
# Start all services in background
docker compose up -d

# Verify all services are healthy (wait for start up)
docker compose ps
```

### Step 4: Access Airflow UI

1. Open http://localhost:8080 in your browser
2. Login with `airflow` / `airflow`
3. Verify the DAG `breweries` appears in the DAGs list

### Step 5: Trigger the Pipeline

1. In Airflow UI, navigate to **DAGs**
2. Find and click on **breweries**
3. The DAG will be running/finished as it runs on deploy
4. Click the **Trigger DAG** button to run it again

### Step 6: Verify Data Output

After successful run:

```bash
# Check storage structure
tree storage/

# Expected output:
# storage/
# ├── bronze/
# │   └── openbrewerydb/
# │       └── breweries/
# │           └── ingest_date=2026-02-26/
# │               └── run_id=<run_id>/
# │                   └── data.json
# ├── silver/
# │   └── breweries/
# │       ├── country=United States/
# │       │   ├── state=California/
# │       │   │   └── data.parquet
# │       │   ├── state=Texas/
# │       │   │   └── data.parquet
# │       │   └── ...
# │       └── country=Brazil/
# │           └── state=Santa Catarina/
# │               └── data.parquet
# └── gold/
#     └── breweries/
#         └── aggregated/
#             └── breweries_by_type_country_state.parquet
```

### Step 7: Stop the Stack

```bash
# Stop all containers (data will be kept)
docker compose down
```

---

## Monitoring and Alerting

### Orchestration-Level Alerting

The DAG counts with built-in Airflow alerting:
- E-mail notifications: Sends e-mail alerts to @authors future coorporate e-mail: pedro.virgilio@bees.com.
- Retrying: Automatically retries failed tasks up to 2 times (first run + 2 retries) without repeating e-mail alerts.

Access the Airflow UI at http://localhost:8080 to monitor DAG runs, task status, and execution logs.

### Data Quality Monitoring and Alerting

The pipeline applies contract validation at the ingestion layer, where it:
- Validates data structure and required fields
- Approaches differently hard fails (incorrect type and empty data) and soft fails (missing fields)
- Logs validation warnings for auditing

Alerting to be done:
1- Record validation rate: Alert if validation rate drops bellow a given treshold (e.g. 90%)
2- Records droped: Alert if soft failures rate exceed a given treshold (e.g. 10%)

### Pipeline Performance Monitoring

The pipeline can emit metrics at key points:
- Bronze layer: Records ingested count, records filtered out (soft fails)
- Silver layer: Records remaining after deduplication, records with missing country/state (marked as "unknown"), partitions created count
- Gold layer: Aggregated brewery count by type and location, appearence of new locations or types

Alerting to be done:
1- If number of records ingested in bronze is too high or too low
2- If number of dropped rows or "unknown" marked rows exceeds a given treshold (e.g. 10%)
3- If aggregated count is too high or too low
4- If any new location or new country/state appears (possible data quality issue)

### API Healt and Reliability

The API client can log retry attempts, hard failing if retrys exceed stated limit (3 attempts).
- Logged retry attempts can be used to track API reliability
- Hard fail if all retries fail stops the execution of the pipeline if data is unreachable.

Alerting to be done:
1- Track retry attempt frquency over time
2- Alert if any run requires more than a single retry to run, to catch any API issues as soon as possible

### Current status

Currently Airflow e-mail alerting is configured. Custom data quality and metrics can be implemented using xcom_push() and xcom_pull() to hold metrics and create sensors/alerts based on the values.

---

## Design Decisions

This section documents the main architectural and implementation choices made in this project, and why they were selected for this case.

### Medallion Architecture and its Boundaries
Decision: keep the 3 layers as separate pipeline steps.
Why:
- Task-level observability: to be able to see exactly at what point a failure has occured.
- Non dependant retries: to be able to retry specific tasks in case they fail, without re-running the entire pipeline.
- Data Checkpoint: To have the structured and cleaned data ready as a checkpoint for any other usage.
- Testing: Each layer can be tested individualy.

### Orchestration with Apache Airflow
Decision: Use Airflow with CeleryExecutor.
Why:
- Distributed execution: Workers process tasks in parallel, allowing better scalability.
- Monitoring: Built-in UI, logs and alerting hooks.
- Ecosystem: Counts with an extensive list of plugins (e-mail, slack, cloud storages, etc.)

### Storage: local filesystem as mocked datalake
Decision: use a local 'storage/' directory mounted to containers as the lake root.
Why:
- Simillar structure to ADLS and S3 (folders as prefixes).
- Easily swapable to a cloud storage by changing the filesystem adapter.

### Partitioning Strategy
Decision: Partition silver layer by 'country' and 'state', but not by 'city'.
Why:
- Tradeoff between scan reduction and small file explosion.
- In this specific scenario, with a local filesystem, partitioning by city as well creates too many small files

### File Formats
Decision: Use JSON on bronze layer and parquet on both silver and gold.
Why:
- JSON on bronze layer, other than mandatory by case instructions, is the original payload format, assuring auditability.
- Parquet because, as requested, is a columnar format and also happens to be compact, efficient and aligns well with datalake access patterns.

### Data Quality strategy
Decision: Apply contract checks early and use both hard and soft failures.
Why:
- Preventing corruption on downstream layers with structurally invalid data.
- Improve ingestion resilience and catching anomalies early.
- Allows good portions of the data to proceed through the pipeline despite partial failing.

### Schema and Data Standardization
Decision: Select a minimal set of columns ASAP, deduplicate by id, drop rows with no country or state.
Why:
- Working with a lighter dataframe as soon as possible to optmize memory and performance.
- Assuring no rows with missing country or state will break the grouping/partitioning step.

### Naming convention and use of run-id
Decision: User run_id from Airflow context to isolate batches only in bronze.
Why:
- Guarantees each batch is isolated in each run.
- Makes debugging easier since it allows mapping of which run generated each batch.
- Silver is partitioned by business dimensions only, ignoring query-id, to avoid multiple versions of the same data-set and to be easier to query from.
