# Azure Databricks Data Ingestion & ETL Pipeline — Design Document (v1)

## 1. Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) on Azure Databricks, ingesting data from two source systems — **Salesforce** (via Bulk API) and **PMM** (via REST API) — and transforming it into curated Delta tables managed by **Unity Catalog**.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION LAYER                           │
│                   Databricks Workflows (multi-task)                  │
│                                                                      │
│  ┌──────────────────────┐    ┌──────────────────────┐               │
│  │ salesforce_ingestion │    │   pmm_ingestion      │               │
│  │      (notebook)      │    │     (notebook)        │               │
│  └─────────┬────────────┘    └──────────┬───────────┘               │
│            │                            │                            │
│            └──────────┬─────────────────┘                            │
│                       ▼                                              │
│            ┌──────────────────────┐                                  │
│            │     etl_pipeline     │                                  │
│            │     (notebook)       │                                  │
│            └──────────────────────┘                                  │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                │
│                                                                      │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐    │
│  │   RAW (Bronze)  │   │  SILVER         │   │   GOLD          │    │
│  │   ADLS / DBFS   │   │  Delta Tables   │   │  Delta Tables   │    │
│  │                 │   │  (Unity Catalog)│   │  (Unity Catalog)│    │
│  │ /raw/salesforce │   │  silver.sales_* │   │  gold.*         │    │
│  │ /raw/pmm        │   │  silver.pmm_*   │   │                 │    │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

## 2. Architecture Layers

### 2.1 Raw Layer (Bronze)
- **Salesforce**: Daily full/incremental extract via Salesforce Bulk API 2.0, saved as **Parquet** partitioned by `trade_date`.
- **PMM**: Daily REST API extract, saved as **Parquet** partitioned by `trade_date`.
- Storage path: `abfss://<container>@<storage>.dfs.core.windows.net/raw/{salesforce,pmm}/trade_date={YYYY-MM-DD}/`

### 2.2 Silver Layer (Curated)
- Reads raw Parquet from both sources.
- Applies: schema validation, deduplication, null handling, business-rule filtering.
- Joins Salesforce + PMM data on common keys.
- Performs aggregations (daily summaries, metrics).
- Writes as **Delta tables** registered in **Unity Catalog** (`<catalog>.silver.<table_name>`).

### 2.3 Gold Layer (Aggregated — future)
- Business-level aggregated views for reporting/BI consumption.

## 3. Technology Stack

| Component          | Technology                                |
|--------------------|-------------------------------------------|
| Compute            | Azure Databricks (Spark clusters)         |
| Orchestration      | Databricks Workflows                      |
| Ingestion (SF)     | Salesforce Bulk API 2.0 + simple-salesforce |
| Ingestion (PMM)    | requests + REST API pagination            |
| Raw Storage        | ADLS Gen2 / DBFS (Parquet)                |
| Silver Storage     | Delta Lake (Unity Catalog)                |
| CI/CD              | GitHub Actions                            |
| Packaging          | Python Wheel (.whl)                       |
| Deployment         | Databricks Asset Bundle (DAB) — recommended |
| Deployment (legacy)| Databricks REST API                       |

## 4. Project Structure

```
azure-datbricks-ingestion-etl-pipeline/
├── DESIGN.md                          # This document
├── README.md                          # Setup & usage instructions
├── Makefile                           # Local dev & deploy commands
├── requirements.txt                   # Python dependencies
├── setup.py                           # Wheel packaging (with console_scripts entry points)
├── config.example.yaml                # Configuration template
├── databricks.yml                     # Databricks Asset Bundle (DAB) root config
│
├── src/
│   └── bank_etl/                      # Top-level package (src/ is source root only)
│       ├── __init__.py
│       ├── config.py                  # Configuration loader
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── auth.py                # OAuth2 / token management
│       │   ├── salesforce_bulk.py     # Salesforce Bulk API 2.0 client
│       │   ├── pmm_api.py             # PMM REST API client
│       │   └── base_rest.py           # Generic REST pagination helper
│       │
│       ├── raw_writer/
│       │   ├── __init__.py
│       │   └── parquet_writer.py      # Write DataFrames as Parquet to raw layer
│       │
│       ├── etl/
│       │   ├── __init__.py
│       │   ├── validator.py           # Schema & data quality validation
│       │   ├── transformer.py         # Filter, join, aggregation logic
│       │   └── delta_writer.py        # Write to Delta tables with Unity Catalog
│       │
│       ├── orchestrators/             # Shared business logic (reused by notebooks & entry_points)
│       │   ├── __init__.py
│       │   ├── salesforce_runner.py   # Salesforce ingestion workflow
│       │   ├── pmm_runner.py          # PMM ingestion workflow
│       │   └── etl_runner.py          # ETL pipeline workflow
│       │
│       ├── entry_points/              # python_wheel_task entry points (thin wrappers)
│       │   ├── __init__.py
│       │   ├── salesforce_ingestion_entry.py
│       │   ├── pmm_ingestion_entry.py
│       │   └── etl_pipeline_entry.py
│       │
│       └── utils/
│           ├── __init__.py
│           ├── spark_utils.py         # Spark session builder
│           └── logging_utils.py       # Logging configuration
│
├── notebooks/                         # notebook_task entry points (thin wrappers)
│   ├── salesforce_ingestion.py        # Databricks notebook: SF → Raw
│   ├── pmm_ingestion.py               # Databricks notebook: PMM → Raw
│   └── etl_pipeline.py                # Databricks notebook: Raw → Silver
│
├── resources/                         # DAB job definitions (YAML)
│   ├── daily_ingestion_etl_pipeline.job.yml
│   └── etl_only_pipeline.job.yml
│
├── workflows/                         # Legacy JSON workflow definitions
│   ├── ingestion_workflow.json        # Databricks Workflow: daily ingestion
│   └── etl_workflow.json              # Databricks Workflow: ETL pipeline
│
├── .github/
│   └── workflows/
│       ├── deploy-dab-pipeline.yml         # CI/CD via DAB (recommended)
│       └── deploy-databricks-pipeline.yml  # CI/CD via REST API (legacy)
│
└── scripts/
    └── deploy_workflows.py            # Databricks REST API deployment helper (legacy)
```

## 5. Architecture: Orchestrator Pattern (Shared Business Logic)

All business logic lives in reusable **orchestrator modules** under [`src/bank_etl/orchestrators/`](src/bank_etl/orchestrators/). Each orchestrator exposes a single `run(**kwargs) -> dict` function that contains the complete workflow. Both notebooks and entry_points are **thin wrappers** (~15–20 lines) that only handle parameter plumbing (widgets or argparse) and delegate everything else to the same orchestrator.

```
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR LAYER                        │
│              src/bank_etl/orchestrators/*.py                 │
│                                                              │
│  salesforce_runner.run()   pmm_runner.run()   etl_runner.run()│
│        ▲                       ▲                    ▲        │
│        │                       │                    │        │
│  ┌─────┴──────┐          ┌─────┴──────┐      ┌─────┴──────┐ │
│  │ notebooks/ │          │ notebooks/ │      │ notebooks/ │ │
│  │ (widgets)  │          │ (widgets)  │      │ (widgets)  │ │
│  └────────────┘          └────────────┘      └────────────┘ │
│  ┌────────────┐          ┌────────────┐      ┌────────────┐ │
│  │entry_points│          │entry_points│      │entry_points│ │
│  │ (argparse) │          │ (argparse) │      │ (argparse) │ │
│  └────────────┘          └────────────┘      └────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Key benefits:**
- **DRY**: Business logic is written once, tested once, and reused everywhere.
- **Testable**: Orchestrators are plain Python — testable with `pytest` without Databricks.
- **Consistent**: Notebooks and wheel tasks produce identical results since they call the same `run()` function.
- **Maintainable**: Fix a bug in one place; both execution modes get the fix automatically.

## 6. Task Execution: Notebook vs Python Wheel

Databricks Workflows support two task types for running Python code. This project provides **both** so teams can choose based on their needs. Both delegate to the same orchestrator modules.

### 6.1 `notebook_task` (default — interactive)

- Uses `dbutils.widgets` for parameter passing
- Produces cell-by-cell output in the Databricks UI
- Supports interactive re-run of individual cells for debugging
- Defined in [`notebooks/`](notebooks/) as Databricks notebook files (~15 lines each)
- Calls the same `bank_etl.orchestrators.*.run()` function as the wheel tasks

### 6.2 `python_wheel_task` (alternative — stateless)

- Uses `argparse` for CLI-style parameter passing
- Runs as a plain Python function — no `dbutils` dependency
- **Testable locally** with `pytest` without a Databricks runtime
- **CI-friendly**: the same entry point can run in GitHub Actions for integration tests
- Defined in [`src/bank_etl/entry_points/`](src/bank_etl/entry_points/) as standard Python modules (~20 lines each)
- Registered as `console_scripts` in [`setup.py`](setup.py)
- Calls the same `bank_etl.orchestrators.*.run()` function as the notebooks

### 6.3 Comparison

| Feature | `notebook_task` | `python_wheel_task` |
|---------|----------------|---------------------|
| Parameter passing | `dbutils.widgets` | `argparse` (CLI args) |
| Interactive debugging | ✅ Cell-by-cell in UI | ❌ Logs only |
| Local testing | ❌ Requires Databricks | ✅ `pytest` on laptop |
| CI integration | ❌ Notebook-specific | ✅ Standard Python |
| State isolation | ⚠️ Cells share state | ✅ Stateless function |
| UI output | ✅ Rich cell output | ⚠️ stdout only |
| Business logic | Delegates to orchestrator | Delegates to orchestrator |

### 6.4 Switching Between Task Types

In the DAB job YAMLs ([`resources/daily_ingestion_etl_pipeline.job.yml`](resources/daily_ingestion_etl_pipeline.job.yml) and [`resources/etl_only_pipeline.job.yml`](resources/etl_only_pipeline.job.yml)), each task has both options. Comment out one block and uncomment the other:

```yaml
# -- Option A: notebook_task (default) --
notebook_task:
  notebook_path: ${workspace.root_path}/notebooks/salesforce_ingestion
  source: WORKSPACE
  base_parameters:
    config_path: "/Workspace/Shared/pipeline_config.yaml"
    trade_date: "{{job.parameter.trade_date}}"

# -- Option B: python_wheel_task --
# python_wheel_task:
#   package_name: databricks_ingestion_etl_pipeline
#   entry_point: bank_etl.entry_points.salesforce_ingestion_entry:main
#   parameters:
#     - "--config_path"
#     - "/Workspace/Shared/pipeline_config.yaml"
#     - "--trade_date"
#     - "{{job.parameter.trade_date}}"
```

## 7. Data Flow

All data flows are implemented in the orchestrator modules under [`src/bank_etl/orchestrators/`](src/bank_etl/orchestrators/). Notebooks and entry_points are thin wrappers that delegate to these orchestrators.

### 7.1 Salesforce Ingestion Flow
```
1. Databricks Workflow triggers salesforce_ingestion (notebook or wheel task)
2. Thin wrapper delegates to bank_etl.orchestrators.salesforce_runner.run()
3. Orchestrator reads config (SF credentials, objects to extract, trade_date)
4. SalesforceBulkClient authenticates via OAuth2 (JWT or password grant)
5. Creates Bulk API 2.0 job for each configured object
6. Downloads results as JSON, converts to Spark DataFrame
7. Validates schema, adds metadata columns (ingestion_ts, trade_date)
8. Writes as Parquet to /raw/salesforce/trade_date={date}/
```

### 7.2 PMM Ingestion Flow
```
1. Databricks Workflow triggers pmm_ingestion (notebook or wheel task)
2. Thin wrapper delegates to bank_etl.orchestrators.pmm_runner.run()
3. Orchestrator reads config (PMM base URL, endpoints, trade_date)
4. PmmApiClient authenticates via API key / OAuth2
5. Paginates through all records for each endpoint
6. Converts JSON response to Spark DataFrame
7. Validates schema, adds metadata columns
8. Writes as Parquet to /raw/pmm/trade_date={date}/
```

### 7.3 ETL Flow (Raw → Silver)
```
1. Databricks Workflow triggers etl_pipeline (notebook or wheel task, depends on both ingestions)
2. Thin wrapper delegates to bank_etl.orchestrators.etl_runner.run()
3. Orchestrator reads raw Parquet for trade_date from both /raw/salesforce and /raw/pmm
4. Validates data quality (null checks, schema enforcement, range checks)
5. Applies business filters (e.g., exclude test records, inactive accounts)
6. Joins Salesforce + PMM datasets on common keys
7. Performs aggregations (daily metrics, summaries)
8. Writes result as Delta table to Unity Catalog (<catalog>.silver.<table>)
9. Runs OPTIMIZE on Delta tables
```

## 8. Unity Catalog Integration

Silver layer tables are registered under Unity Catalog with three-level namespace:
- `catalog`: Configurable (e.g., `main` or `prod`)
- `schema`: `silver`
- `table`: e.g., `salesforce_accounts`, `pmm_metrics`, `joined_daily_summary`

Delta writer uses:
```python
df.write.mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.silver.{table_name}")
```

## 9. CI/CD Pipeline (GitHub Actions)

Two deployment methods are available. DAB is the recommended approach.

### 9.1 Databricks Asset Bundle (DAB) — Recommended

```
Push to main →
  Validate bundle (databricks bundle validate) →
  Deploy to Dev (databricks bundle deploy -t dev) →
  [Manual approval] Deploy to Staging →
  [Manual approval] Deploy to Prod
```

- **Authentication**: OAuth (service principal) via `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`
- **Workflow file**: [`.github/workflows/deploy-dab-pipeline.yml`](.github/workflows/deploy-dab-pipeline.yml)
- **Key commands**:
  - `databricks bundle validate -t <target>` — validate bundle schema
  - `databricks bundle deploy -t <target>` — deploy all resources (wheel, notebooks, jobs)
  - `databricks bundle run -t <target> <job_name>` — trigger a job run
  - `databricks bundle destroy -t <target>` — tear down all deployed resources

### 9.2 REST API (Legacy)

```
Push to main →
  Validate (syntax, lint) →
  Build Python Wheel →
  Upload Wheel to DBFS / Workspace →
  Deploy/Update Databricks Workflows via REST API
```

- **Authentication**: Personal Access Token (PAT) via `DATABRICKS_TOKEN`
- **Workflow file**: [`.github/workflows/deploy-databricks-pipeline.yml`](.github/workflows/deploy-databricks-pipeline.yml)
- **Deployment script**: [`scripts/deploy_workflows.py`](scripts/deploy_workflows.py)

### 9.3 Target Environments

| Target    | Workspace Path                                              | Trigger              |
|-----------|-------------------------------------------------------------|----------------------|
| `dev`     | `/Workspace/Shared/.bundle/ingestion-etl-pipeline/dev`      | Auto on push to main |
| `staging` | `/Workspace/Shared/.bundle/ingestion-etl-pipeline/staging`  | Manual (workflow_dispatch) |
| `prod`    | `/Workspace/Shared/.bundle/ingestion-etl-pipeline/prod`     | Manual (workflow_dispatch) |

## 10. Configuration

All sensitive values (credentials, storage keys) are stored as **Databricks Secrets** and referenced via `dbutils.secrets.get()`. Non-sensitive config is in `config.yaml` mounted to the cluster or passed as notebook parameters.

## 11. Scheduling

| Job Name                          | Schedule (UTC) | Description                        |
|-----------------------------------|----------------|------------------------------------|
| `daily_ingestion_etl_pipeline`    | 02:00 daily    | SF + PMM ingestion (parallel) → ETL |
| `etl_only_pipeline`               | 04:00 daily    | Standalone ETL (Raw → Silver)      |

The `daily_ingestion_etl_pipeline` job runs three tasks:
1. **salesforce_ingestion** (02:00) — extracts Salesforce data to raw Parquet
2. **pmm_ingestion** (02:00, parallel) — extracts PMM data to raw Parquet
3. **etl_pipeline** (depends on both above) — validates, transforms, and writes to Silver Delta tables

## 12. Error Handling & Monitoring

- All notebooks use try/except with structured logging
- Failed tasks in Databricks Workflows can trigger email alerts
- Retry policy: 3 retries with 5-minute backoff
- Data quality metrics logged per run (row counts, null percentages)
