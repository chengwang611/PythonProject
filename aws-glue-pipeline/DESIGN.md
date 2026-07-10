# AWS Glue ETL Pipeline — Design Document

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Design Decisions & Justifications](#design-decisions--justifications)
4. [Component Deep Dive](#component-deep-dive)
5. [Exactly-Once Processing Guarantee](#exactly-once-processing-guarantee)
6. [Lake Formation & Data Governance](#lake-formation--data-governance)
7. [Security Model](#security-model)
8. [Known Limitations & Improvement Areas](#known-limitations--improvement-areas)
9. [Alternative Solutions Considered](#alternative-solutions-considered)
10. [Future Roadmap](#future-roadmap)

---

## Overview

The AWS Glue ETL Pipeline is a **serverless, event-driven** data ingestion system that automatically processes CSV files dropped into an S3 bucket. It uses **AWS Glue** (serverless Apache Spark) for ETL processing, **S3 conditional put** for distributed locking (exactly-once semantics), and **AWS Lake Formation** for fine-grained data governance on the output.
[Makefile](..%2Faws-sagemaker-pipeline%2FMakefile)
### Key Design Goals

| Goal | Approach |
|------|----------|
| **Serverless** | No EC2/EMR clusters to manage; Glue provisions compute on demand |
| **Exactly-once processing** | Two-layer defense: S3 conditional put (Lambda) + S3 marker (Glue job) |
| **Cost-efficient** | Pay per DPU-second; no idle compute costs |
| **Fine-grained access control** | Lake Formation governs all read access to processed data |
| **Observable** | CloudWatch logs, Glue job metrics, `_MANIFEST.json` per job run |
| **CI/CD ready** | CloudFormation IaC, GitHub Actions OIDC deployment |

---

## Architecture

```
                              ┌─────────────────────────────────────────────────────────────────────────────┐
                              │                        AWS LAKE FORMATION (Governance Layer)                 │
                              │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
                              │  │  Column-Level       │  │  Row-Level          │  │  Cell-Level         │  │
                              │  │  Security           │  │  Filtering          │  │  Masking (PII)      │  │
                              │  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘  │
                              └─────────────────────────────────────────────────────────────────────────────┘
                                                          │
                                                          │ Governs Access
                                                          ▼
┌──────────────────┐   S3 Event       ┌──────────────┐   SQS Message    ┌──────────────────────────┐
│   S3 Bucket      │  (_COMPLETE)     │  SQS Queue   │ ───────────────► │    Lambda                │
│  ┌────────────┐  │ ───────────────► │ (Ingestion)  │                  │  (Folder-Level Sentinel) │
│  │ part-01.csv│  │                  │              │                  │                          │
│  │ part-02.csv│  │                  │ Filter:      │                  │  1. Extract folder       │
│  │ part-NN.csv│  │                  │ *_COMPLETE   │                  │  2. List .csv files      │
│  │ _COMPLETE  │  │                  │              │                  │  3. Folder-level lock    │
│  └────────────┘  │                  └──────┬───────┘                  │  4. Start Glue job       │
└──────────────────┘                        │                          └──────────┬───────────────┘
                                            │ DLQ                                 │
                                       ┌────▼────┐                         ┌──────▼──────┐
                                       │   DLQ   │                         │  S3 Bucket  │
                                       │ (Failed)│                         │  (_locks/)  │
                                       └─────────┘                         └──────┬──────┘
                                                                                 │
                                                                     ┌───────────▼───────────┐
                                                                     │  IfNoneMatch='*'      │
                                                                     │  (Folder-Level Lock)  │
                                                                     └───────────┬───────────┘
                                                                                 │
                                                                    ┌────────────▼────────────┐
                                                                    │  Glue: StartJobRun      │
                                                                    │  (All files in folder)  │
                                                                    └────────────┬────────────┘
                                                                                │
                                                                        ┌───────▼────────┐
                                                                        │  S3 Processed   │
                                                                        │  (Parquet Out)  │
                                                                        └───────┬────────┘
                                                                                │
                                                                   ┌────────────▼────────────┐
                                                                   │   AWS Glue Data Catalog  │
                                                                   │  ┌────────────────────┐  │
                                                                   │  │ Database:          │  │
                                                                   │  │ etl_processed_data │  │
                                                                   │  │                    │  │
                                                                   │  │ Table: csv_output  │  │
                                                                   │  │ (Parquet, LF-gov)  │  │
                                                                   │  └────────────────────┘  │
                                                                   └────────────┬────────────┘
                                                                                │
                                                          ┌─────────────────────┼─────────────────────┐
                                                          │                     │                     │
                                                          ▼                     ▼                     ▼
                                              ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
                                              │  Amazon Athena   │  │ Redshift Spectrum│  │   Amazon EMR     │
                                              │  (SQL Queries)   │  │  (Federated)     │  │  (Spark/Hive)    │
                                              └────────┬─────────┘  └──────────────────┘  └──────────────────┘
                                                       │
                                                       │ SQL via Lake Formation
                                                       ▼
                                              ┌──────────────────┐
                                              │  Data Analyst /  │
                                              │  BI Tool         │
                                              │  (QuickSight,    │
                                              │   Tableau, etc.) │
                                              └──────────────────┘
```

### Data Flow (Step by Step)

1. **Upstream uploads all CSV files** to a folder in the S3 data bucket (`s3://{data-bucket}/incoming/2026-07-10/part-01.csv`, `part-02.csv`, ...)
2. **Upstream uploads `_COMPLETE` sentinel file** as the LAST file in the folder, signaling all data is ready
3. **S3 event notification** sends a message to the SQS ingestion queue (filtered for `_COMPLETE` suffix)
4. **Lambda function** ([`glue_trigger.py`](lambda/glue_trigger.py)) is triggered by SQS:
   - **Extracts folder prefix** from the sentinel path (e.g., `incoming/2026-07-10/_COMPLETE` → `incoming/2026-07-10/`)
   - **Exactly-Once Check**: S3 conditional put with `IfNoneMatch='*'` — atomic distributed lock at the **folder level**
   - If lock exists → skip (folder already processed)
   - If lock is new → acquire and proceed
   - **Lists all `.csv` files** in the folder via `s3.list_objects_v2()`
   - Optionally **validates** file count against sentinel metadata
   - Starts Glue job with all file paths as a JSON array
   - On failure → releases the S3 lock so the folder can be retried
5. **Glue job** ([`csv_etl_job.py`](glue-jobs/csv_etl_job.py)) runs serverless Spark:
   - **Idempotency Check**: Checks for existing `_SUCCESS` marker before processing
   - **Reads all CSV files** from the JSON array, unions them into a single DataFrame
   - Applies transformations (clean, aggregate, or passthrough)
   - Writes Parquet output to processed S3 bucket
   - Writes `_SUCCESS` dedup marker and `_MANIFEST.json`
   - **Triggers Glue Crawler** to auto-discover the Parquet schema
6. **SQS message is deleted** after successful processing
7. **Failed messages** go to a Dead Letter Queue (DLQ) after 5 retries
8. **Glue Crawler** scans the processed Parquet files, infers the schema, and creates/updates tables in the Glue Data Catalog
9. **Lake Formation** governs all access — enforcing column-level, row-level, and cell-level security
10. **Amazon Athena** (and Redshift Spectrum, EMR) queries the data through Lake Formation
11. **BI tools** (QuickSight, Tableau) connect via Athena JDBC/ODBC

---

## Design Decisions & Justifications

### 1. Why Glue Instead of EMR?

| Factor | EMR | Glue (Chosen) | Justification |
|--------|-----|---------------|---------------|
| **Startup time** | 5–10 minutes | 1–2 minutes | For per-file processing, EMR startup dominates cost and latency |
| **Pricing model** | Per EC2 instance-hour | Per DPU-second | No idle cost; ideal for sporadic file drops |
| **Management** | EC2 instances, security groups, bootstrap actions | Fully serverless | Reduces operational burden |
| **Scaling** | Manual or auto-scaling rules | Automatic (serverless) | No capacity planning needed |
| **VPC requirement** | Required | Optional | Simpler networking for simple CSV ETL |
| **Cost for 1,000 files** | ~$50–200 | ~$5–20 | 10x cost reduction for typical workloads |

**Trade-off**: Glue has less control over the Spark environment (no custom JARs, limited bootstrap). For this pipeline's use case (CSV → Parquet with basic transforms), Glue's constraints are acceptable.

### 2. Why S3 Conditional Put for Exactly-Once (Not DynamoDB)?

The pipeline uses `IfNoneMatch='*'` on S3 `put_object` as a distributed lock:

```python
s3_client.put_object(
    Bucket=lock_bucket,
    Key=f"_locks/{file_hash}.lock",
    IfNoneMatch='*',  # Atomic: only succeed if object does NOT exist
)
```

| Approach | Pros | Cons |
|----------|------|------|
| **S3 Conditional Put (chosen)** | No additional infrastructure; atomic at S3 API level; lock auto-expires via S3 lifecycle | S3 eventual consistency on reads (mitigated by strong consistency on conditional writes); lock objects consume storage |
| **DynamoDB** | Strong consistency; TTL-based expiry; conditional writes | Additional infrastructure cost; another service to manage; requires table provisioning |
| **Redis/ElastiCache** | Sub-millisecond latency; TTL built-in | VPC required; significant cost; overkill for file-level dedup |

**Justification**: S3 conditional put provides atomic check-and-set semantics without any additional infrastructure. The lock objects are tiny (~200 bytes JSON) and auto-expire via S3 lifecycle policies (7 days). The trade-off is that S3 read-after-write consistency for lock checks is not guaranteed, but this is mitigated by the two-layer defense (see below).

### 3. Why Two-Layer Exactly-Once Defense?

| Layer | Location | Mechanism | Failure Mode Covered |
|-------|----------|-----------|---------------------|
| **Layer 1** | Lambda ([`glue_trigger.py:78`](lambda/glue_trigger.py:78)) | S3 conditional put `IfNoneMatch='*'` | Duplicate S3 events, Lambda retries, SQS redelivery |
| **Layer 2** | Glue Job ([`csv_etl_job.py:114`](glue-jobs/csv_etl_job.py:114)) | S3 `_SUCCESS` marker check | Glue job retries, Lambda crash after Glue submit |

**Justification**: A single layer is insufficient. If the Lambda crashes after submitting the Glue job but before deleting the SQS message, the SQS message will be redelivered. Layer 1 (S3 lock) prevents the Lambda from submitting a duplicate Glue job. If the Glue job itself is retried (e.g., transient Spark failure), Layer 2 (S3 marker) prevents duplicate output.

### 4. Why SQS Between S3 and Lambda?

S3 event notifications can be sent directly to Lambda, but using SQS as an intermediary provides:

- **Buffering**: Handles bursts of file uploads without throttling Lambda
- **Retry/DLQ**: Failed messages automatically go to DLQ after 5 retries
- **Decoupling**: S3 doesn't need to know about Lambda; Lambda doesn't need S3 event schema knowledge
- **Batching**: Lambda can process multiple S3 events in a single invocation (currently `BatchSize: 1` for simplicity)

### 5. Why Lake Formation for Access Control?

| Capability | S3 Bucket Policies | Lake Formation (Chosen) |
|-----------|-------------------|------------------------|
| **Column-level security** | ❌ Not possible | ✅ Hide PII columns from analysts |
| **Row-level filtering** | ❌ Not possible | ✅ Filter by region/department |
| **Cell-level masking** | ❌ Not possible | ✅ Mask SSN, credit card numbers |
| **Cross-account sharing** | Complex IAM role chaining | Simplified LF grants |
| **Audit trail** | CloudTrail S3 events only | Built-in Lake Formation audit |
| **Tag-based access** | ❌ Not supported | ✅ LF-TBAC |

**Justification**: The processed data may contain PII or sensitive business metrics. Lake Formation provides database-level access control that works uniformly across Athena, Redshift Spectrum, EMR, and Glue — without modifying application code.

---

## Component Deep Dive

### Lambda Function: [`glue_trigger.py`](lambda/glue_trigger.py)

**Purpose**: SQS consumer that acquires a distributed lock and triggers Glue jobs.

**Key Design Elements**:

| Element | Implementation | Rationale |
|---------|---------------|-----------|
| **Lock acquisition** | `s3.put_object(IfNoneMatch='*')` | Atomic at S3 API level; no race conditions |
| **Lock release** | `s3.delete_object()` on failure | Allows retry on transient Glue failures |
| **Lock payload** | JSON with `original_path`, `locked_at`, `environment` | Human-readable for debugging |
| **File dedup key** | SHA-256 of `s3://bucket/key` | Deterministic; same file always produces same hash |
| **S3 unavailability** | Falls through to at-least-once | Prevents pipeline from blocking on S3 outages |
| **SQS message deletion** | After all files in batch processed | Prevents message loss on partial failure |

**Error Handling**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Lambda Error Handling                        │
├──────────────────┬──────────────────────┬───────────────────────┤
│ Failure Scenario │ Lock State           │ Result                │
├──────────────────┼──────────────────────┼───────────────────────┤
│ Lambda crash     │ No lock              │ SQS retry → reprocess │
│ before lock      │                      │                       │
├──────────────────┼──────────────────────┼───────────────────────┤
│ Lambda crash     │ Lock exists          │ Next retry skips      │
│ after lock       │                      │ (exactly-once)        │
├──────────────────┼──────────────────────┼───────────────────────┤
│ Glue start fails │ Lock released        │ SQS retry → reprocess │
├──────────────────┼──────────────────────┼───────────────────────┤
│ S3 unavailable   │ Exception caught     │ Falls through to      │
│ during lock      │ → returns True       │ at-least-once         │
└──────────────────┴──────────────────────┴───────────────────────┘
```

### Glue Job: [`csv_etl_job.py`](glue-jobs/csv_etl_job.py)

**Purpose**: Serverless PySpark ETL that reads CSV, transforms, and writes Parquet.

**Key Design Elements**:

| Element | Implementation | Rationale |
|---------|---------------|-----------|
| **Idempotency** | S3 `_SUCCESS` marker check before processing | Second line of defense for exactly-once |
| **Transform types** | `clean`, `aggregate`, `passthrough` | Covers 90% of CSV ETL use cases |
| **Output format** | Parquet with Snappy compression | Columnar, compressed, Athena-compatible |
| **Partitioning** | Optional `partition_by` columns | Enables partition pruning in Athena |
| **Manifest** | `_MANIFEST.json` per job run | Audit trail with record counts, duration, status |
| **GlueContext** | Falls back to standard SparkSession | Works both in Glue and local testing |
| **Spark config** | AQE enabled, Kryo serializer, dynamic partition overwrite | Performance best practices |

**Transform Pipeline**:

```
CSV Input
    │
    ▼
┌─────────────────┐
│  Read CSV        │  PERMISSIVE mode, corrupt record column
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Idempotency     │  Check _SUCCESS marker → skip if exists
│  Check           │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transform       │  clean / aggregate / passthrough
│  ┌─────────────┐ │
│  │ clean:      │ │  Drop corrupt → drop columns → rename → parse dates → drop nulls → dedup
│  │ aggregate:  │ │  Group by non-numeric → apply agg function
│  │ passthrough:│ │  No changes
│  └─────────────┘ │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Write Parquet   │  Snappy compression, optional partitioning
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Write Manifest  │  _MANIFEST.json + _SUCCESS marker
└─────────────────┘
```

### CloudFormation Stack: [`main-stack.yaml`](cloudformation/main-stack.yaml)

**Resources Created**:

| Resource | Type | Purpose |
|----------|------|---------|
| Data S3 Bucket | `AWS::S3::Bucket` | CSV input with S3 event → SQS notification |
| Processed S3 Bucket | `AWS::S3::Bucket` | Parquet output + lock objects |
| Artifacts S3 Bucket | `AWS::S3::Bucket` | Glue scripts, Spark logs, temp data |
| Ingestion SQS Queue | `AWS::SQS::Queue` | Buffers S3 events with DLQ |
| Dead Letter Queue | `AWS::SQS::Queue` | Captures failed messages after 5 retries |
| Lambda Function | `AWS::Lambda::Function` | SQS consumer → Glue trigger |
| Lambda Event Source | `AWS::Lambda::EventSourceMapping` | SQS → Lambda trigger |
| Glue Job | `AWS::Glue::Job` | Serverless PySpark ETL (Glue 5.0) — triggers crawler after writing |
| Glue Crawler | `AWS::Glue::Crawler` | Auto-discovers Parquet schema, creates/updates catalog tables |
| Glue Database | `AWS::Glue::Database` | `etl_processed_data` catalog database |
| Lake Formation Resource | `AWS::LakeFormation::Resource` | Registered S3 location |
| Lake Formation Permissions | `AWS::LakeFormation::Permissions` | Database-level + all-tables wildcard grants |
| IAM Roles | `AWS::IAM::Role` | Lambda execution, Glue job (with crawler permissions), GitHub OIDC deploy |
| CloudWatch Log Group | `AWS::Logs::LogGroup` | Lambda logs with configurable retention |

---

## Exactly-Once Processing Guarantee

### Two-Layer Defense Architecture

```
                    ┌──────────────────────────────────────┐
                    │         SQS Message Arrives           │
                    └──────────────────┬───────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────┐
                    │  LAYER 1: S3 Conditional Put          │
                    │  (Lambda — glue_trigger.py:78)        │
                    │                                      │
                    │  put_object(IfNoneMatch='*')          │
                    │                                      │
                    │  ┌─ Success ──► Lock acquired ──►     │
                    │  │                                   │
                    │  └─ PreconditionFailed ──► SKIP       │
                    └──────────────────┬───────────────────┘
                                       │ (lock acquired)
                                       ▼
                    ┌──────────────────────────────────────┐
                    │  Start Glue Job                       │
                    │  (Lambda — glue_trigger.py:225)       │
                    └──────────────────┬───────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────┐
                    │  LAYER 2: S3 Marker Check             │
                    │  (Glue Job — csv_etl_job.py:114)      │
                    │                                      │
                    │  Check _SUCCESS marker                │
                    │                                      │
                    │  ┌─ Exists ──► SKIP (already done)    │
                    │  │                                   │
                    │  └─ Missing ──► Process file          │
                    └──────────────────────────────────────┘
```

### Failure Mode Analysis

| # | Failure Scenario | Layer 1 (S3 Lock) | Layer 2 (S3 Marker) | Result |
|---|-----------------|-------------------|---------------------|--------|
| 1 | Lambda crashes before lock | No lock → retry | N/A | **Retried** ✅ |
| 2 | Lambda crashes after lock, before Glue start | Lock exists → stale detection after 1h → delete + re-acquire | N/A | **Retried** ✅ (stale lock recovery) |
| 3 | Lambda crashes after Glue start, before SQS delete | Lock exists (fresh, Glue running) → skip | Marker may exist → skip | **Exactly once** ✅ |
| 4 | Glue job fails (transient) | Lock released by Lambda on start failure → retry | No marker → process | **Retried** ✅ |
| 5 | Glue job fails (permanent) | Lock stays (not stale — Glue was running) | No marker → process | **Locked** (manual intervention or 7-day expiry) |
| 6 | Same folder uploaded twice | Lock from first exists → skip | N/A | **Exactly once** ✅ |
| 7 | S3 duplicate event | Lock from first exists → skip | N/A | **Exactly once** ✅ |
| 8 | S3 unavailable during lock | Exception → fall through | Marker check catches | **At-least-once** ⚠️ |

**⚠️ Scenario 8** is the only case where exactly-once degrades to at-least-once. This is an intentional trade-off: blocking the pipeline during an S3 outage is worse than rare duplicates.

### Stale Lock Detection

The Lambda implements **stale lock detection** to handle the case where it crashes after acquiring the lock but before starting the Glue job (Scenario 2). Without this, the folder would be stuck forever.

**How it works** ([`glue_trigger.py`](lambda/glue_trigger.py)):

1. Before acquiring a lock, the Lambda calls `s3.head_object()` to check if a lock already exists
2. If the lock exists, it reads `LastModified` to calculate the lock's age
3. If `lock_age > LOCK_STALE_TIMEOUT_SECONDS` (default: 3600 = 1 hour), the lock is **stale**:
   - Delete the stale lock
   - Acquire a fresh lock
   - Process the folder
4. If the lock is fresh (≤ 1 hour old), skip — the folder is being processed

**Why 1 hour?** A Glue job that runs longer than 1 hour is unusual for CSV ETL. If a legitimate job runs longer, the lock is still valid because the Glue job releases it on success — the lock only becomes stale if the Lambda crashed and no Glue job was ever started.

---

## Lake Formation & Data Governance

### Authorization Flow

```
                         ┌──────────────────────────────────────────┐
                         │         Lake Formation Policy Engine      │
                         │                                          │
                         │  ┌────────────────────────────────────┐  │
                         │  │  Database: etl_processed_data       │  │
                         │  │  ┌────────────────────────────────┐ │  │
                         │  │  │  Table: csv_output              │ │  │
                         │  │  │                                 │ │  │
                         │  │  │  ├── Column-level: hide PII     │ │  │
                         │  │  │  ├── Row-level: filter by dept  │ │  │
                         │  │  │  └── Cell-level: mask SSN/CC    │ │  │
                         │  │  └────────────────────────────────┘ │  │
                         │  └────────────────────────────────────┘  │
                         └──────────────────┬───────────────────────┘
                                            │
              ┌─────────────────────────────┼─────────────────────────────┐
              │                             │                             │
              ▼                             ▼                             ▼
   ┌──────────────────┐          ┌──────────────────┐          ┌──────────────────┐
   │  Glue Job Role   │          │  Data Analyst     │          │  External Account │
   │  (Writer)        │          │  (Reader)         │          │  (Cross-Account)  │
   │                  │          │                   │          │                   │
   │  Permissions:    │          │  Permissions:     │          │  Permissions:     │
   │  SELECT, INSERT, │          │  SELECT, DESCRIBE │          │  SELECT, DESCRIBE │
   │  ALTER, DROP,    │          │                   │          │                   │
   │  DESCRIBE        │          │                   │          │                   │
   └────────┬─────────┘          └────────┬──────────┘          └────────┬──────────┘
            │                             │                             │
            ▼                             ▼                             ▼
   ┌──────────────────┐          ┌──────────────────┐          ┌──────────────────┐
   │  Writes Parquet  │          │  Amazon Athena   │          │  Amazon Athena   │
   │  to S3 + Catalog │          │  (SQL Queries)   │          │  (Cross-Account) │
   └──────────────────┘          └────────┬─────────┘          └────────┬─────────┘
                                          │                             │
                                          ▼                             ▼
                                 ┌──────────────────┐          ┌──────────────────┐
                                 │  QuickSight /    │          │  QuickSight /    │
                                 │  Tableau / etc.  │          │  Tableau / etc.  │
                                 └──────────────────┘          └──────────────────┘
```

### Lake Formation Resources in CloudFormation

| Resource | CloudFormation Type | Line in [`main-stack.yaml`](cloudformation/main-stack.yaml) | Purpose |
|----------|-------------------|-----------------------------------------------------------|---------|
| Registered S3 Location | `AWS::LakeFormation::Resource` | [442](cloudformation/main-stack.yaml:442) | Governs the processed S3 bucket |
| Glue Database | `AWS::Glue::Database` | [451](cloudformation/main-stack.yaml:451) | `etl_processed_data` catalog database |
| Glue Crawler | `AWS::Glue::Crawler` | (new) | Auto-discovers Parquet schema, creates/updates tables |
| Glue Job DB Grant | `AWS::LakeFormation::Permissions` | (updated) | `CREATE_TABLE`, `ALTER`, `DROP`, `DESCRIBE` on database |
| Glue Job All-Tables Grant | `AWS::LakeFormation::Permissions` | (new) | `SELECT`, `INSERT`, `ALTER`, `DROP`, `DESCRIBE` on all tables (wildcard `*`) |
| Data Location Grant | `AWS::LakeFormation::Permissions` | (updated) | `DATA_LOCATION_ACCESS` |
| External Principal Grant | `AWS::LakeFormation::Permissions` | (updated) | Optional `SELECT` + `DESCRIBE` on all tables (wildcard `*`) |

---

## Security Model

### IAM Roles

| Role | Trust Principal | Key Permissions |
|------|----------------|-----------------|
| **Lambda Execution Role** | `lambda.amazonaws.com` | SQS read/delete, Glue start job, S3 read/write, IAM PassRole |
| **Glue Job Role** | `glue.amazonaws.com` | S3 read/write, Glue catalog CRUD, Lake Formation data access, CloudWatch logs |
| **GitHub Deploy Role** | `token.actions.githubusercontent.com` (OIDC) | CloudFormation, S3, Lambda, SQS, Glue, IAM PassRole |

### Data Encryption

| Data State | Encryption |
|-----------|-----------|
| **S3 at rest** | SSE-S3 (default) — can be upgraded to SSE-KMS |
| **SQS at rest** | SSE-SQS (default) |
| **Glue temp storage** | S3 SSE-S3 |
| **Data in transit** | TLS 1.2+ for all AWS API calls |

### Network Security

- No VPC required (simplifies setup)
- S3 buckets have `PublicAccessBlockConfiguration` blocking all public access
- SQS queue policy restricts senders to the specific S3 bucket
- Lambda runs in AWS-managed VPC

---

## Known Limitations & Improvement Areas

### 1. S3 Eventual Consistency on Lock Reads

**Issue**: S3 conditional put (`IfNoneMatch='*'`) is strongly consistent, but reading the lock object to check its status is eventually consistent. If a lock is deleted and immediately re-checked, a stale read could show the lock as still existing.

**Impact**: Low. The two-layer defense (S3 marker in Glue job) catches any edge cases.

**Improvement**: Add a `LastModified` timestamp check when reading locks to detect stale reads.

### 2. No Schema Evolution

**Issue**: The Glue Catalog table ([`main-stack.yaml:467`](cloudformation/main-stack.yaml:467)) has a fixed schema with only `partition_0` and `partition_1` columns. If the CSV schema changes (new columns added), the Glue table won't reflect it.

**Impact**: Athena queries will only see the columns defined in the table schema.

**Improvement**: Use a Glue Crawler to automatically discover and update the schema after each job run, or implement schema merging in the Glue job.

### 3. Single File Processing (No Batching)

**Issue**: The Lambda event source mapping has `BatchSize: 1` ([`main-stack.yaml:304`](cloudformation/main-stack.yaml:304)), meaning each SQS message is processed individually.

**Impact**: Higher Lambda invocation count; potential for throttling under high load.

**Improvement**: Increase `BatchSize` to 10 and implement batch processing in the Lambda. This would reduce Lambda costs and improve throughput.

### 4. No Glue Job Status Monitoring

**Issue**: The Lambda fires `start_job_run()` and immediately deletes the SQS message — it does not wait for or check the Glue job's completion status.

**Impact**: If the Glue job fails silently, the Lambda won't know and won't release the lock for retry.

**Improvement**: Implement a Step Functions state machine that:
1. Lambda acquires lock → starts Glue job
2. Step Functions polls `glue.get_job_run()` until SUCCEEDED/FAILED
3. On FAILED → release lock, notify via SNS
4. On SUCCEEDED → delete SQS message

### 5. No Data Validation Before Processing

**Issue**: The Glue job reads CSV with `PERMISSIVE` mode and drops corrupt records, but there's no pre-validation of the CSV structure.

**Impact**: Malformed CSVs (wrong delimiter, missing columns) produce empty or partial output without clear error signaling.

**Improvement**: Add a Lambda-based pre-validation step that checks:
- CSV header matches expected columns
- Row count > 0
- File size within expected range

### 6. Lake Formation Table Schema is Static ✅ RESOLVED

**Resolution**: Replaced the static `AWS::Glue::Table` with an `AWS::Glue::Crawler` ([`main-stack.yaml`](cloudformation/main-stack.yaml)). The crawler scans the processed Parquet files after each ETL job, auto-discovers the full schema, and creates/updates tables in the Glue Data Catalog. The Glue job triggers the crawler via `glue:StartCrawler` after writing output.

**Benefits**:
- Schema is always in sync with actual Parquet data
- New columns are automatically discovered and merged
- No manual schema updates needed
- Athena/Redshift/EMR always see the latest columns

**Trade-off**: Crawler runs add ~1-5 minutes of latency before new columns are queryable. The crawler runs asynchronously (fire-and-forget from the Glue job), so ETL throughput is not affected.

### 7. No Cost Monitoring or Budget Alerts

**Issue**: No CloudWatch alarms or budget alerts are configured for Glue DPU consumption.

**Impact**: A runaway job or misconfiguration could lead to unexpected costs.

**Improvement**: Add:
- CloudWatch alarm on Glue job duration > expected
- AWS Budgets alert on Glue spend
- Lambda concurrency limit to prevent SQS flood

### 8. Lock Objects Have No TTL Enforcement

**Issue**: Lock objects rely on S3 lifecycle policies (7-day expiration). If the lifecycle policy fails or is misconfigured, stale locks could accumulate.

**Impact**: Storage cost (negligible for small JSON objects) and potential confusion during debugging.

**Improvement**: Add a Lambda-based lock cleanup function that runs daily and deletes locks older than N hours.

---

## Alternative Solutions Considered

### Alternative 1: EMR-Based Pipeline

**Already implemented** in [`aws-etl-pipeline/`](../aws-etl-pipeline/).

| Aspect | EMR Pipeline | Glue Pipeline (Chosen) |
|--------|-------------|----------------------|
| Startup | 5–10 min | 1–2 min |
| Cost per file | $0.05–0.20 | $0.005–0.02 |
| Custom JARs | ✅ Yes | ❌ No |
| Hudi/Delta/Iceberg | ✅ Yes | ❌ Limited |
| VPC required | ✅ Yes | ❌ No |

**When to use EMR instead**: When you need custom Spark configurations, table formats (Apache Hudi/Iceberg), or long-running streaming jobs.

### Alternative 2: Lambda-Only Processing

Instead of Glue, process CSV files directly in Lambda using Pandas/Polars.

| Aspect | Lambda-Only | Glue (Chosen) |
|--------|------------|---------------|
| Max file size | 10 GB (tmp storage) | Unlimited (Spark distributed) |
| Max runtime | 15 minutes | 48 hours |
| Parallelism | Concurrency limit (1000) | Auto-scaling workers |
| Cost for large files | High (memory × time) | Lower (distributed) |

**When to use Lambda-only**: For small CSV files (< 500 MB) with simple transformations.

### Alternative 3: Kinesis Instead of SQS

| Aspect | SQS (Chosen) | Kinesis |
|--------|-------------|---------|
| Ordering | Best-effort (standard), FIFO available | Strict ordering |
| Replay | No (DLQ only) | Yes (24h–365d retention) |
| Throughput | Unlimited (standard) | Per-shard limits |
| Complexity | Low | Medium (shard management) |
| Cost | $0.40/million requests | $0.015/shard-hour + PUT charges |

**When to use Kinesis**: When you need strict ordering, replay capability, or fan-out to multiple consumers.

### Alternative 4: DynamoDB for Locking

| Aspect | S3 Conditional Put (Chosen) | DynamoDB |
|--------|---------------------------|----------|
| Infrastructure | None (reuses S3) | Table provisioning |
| Consistency | Strong (conditional write) | Strong |
| TTL | S3 lifecycle (days) | DynamoDB TTL (seconds precision) |
| Cost | ~$0.005/million locks | ~$1.25/million writes |
| Latency | ~10–50ms | ~5–10ms |

**When to use DynamoDB**: When you need sub-second lock TTL, higher throughput, or sub-10ms latency.

### Alternative 5: Step Functions for Orchestration

Instead of Lambda directly calling Glue, use Step Functions for the workflow.

| Aspect | Lambda → Glue (Chosen) | Step Functions |
|--------|----------------------|----------------|
| Glue status tracking | ❌ Fire-and-forget | ✅ Poll until complete |
| Error handling | Manual in code | Built-in retry/catch |
| Complexity | Low | Medium |
| Cost | $0.20/million invocations | $0.025/1,000 state transitions |

**When to use Step Functions**: When you need Glue job completion tracking, complex error handling, or multi-step workflows (validate → process → notify).

---

## Design Pattern: Folder-Level Completion Trigger

> **See [`FOLDER_TRIGGER_DESIGN.md`](FOLDER_TRIGGER_DESIGN.md) for the full design document covering:**
> - Sentinel File approach (recommended) with architecture diagram, Lambda code changes, and Glue job modifications
> - DynamoDB File Counter alternative
> - Time Window (EventBridge Scheduler) alternative
> - S3 Batch Operations + Manifest alternative
> - Recommendation matrix and implementation effort comparison

### Quick Summary

The current pipeline triggers on **every individual `.csv` file**. To trigger only when **all files in a folder** have landed, the recommended approach is a **Sentinel File** pattern:

1. **Upstream** uploads all data files, then uploads a `_COMPLETE` marker file last
2. **S3 event** fires only on `_COMPLETE` suffix (change from `.csv`)
3. **Lambda** extracts the folder prefix, lists all `.csv` files in that folder, acquires a folder-level lock, and starts a Glue job with all file paths
4. **Glue job** reads all files, unions them, transforms, and writes a single Parquet output

This requires minimal changes: S3 suffix filter (`_COMPLETE`), Lambda folder-listing logic, and Glue multi-file read support. No new AWS infrastructure needed.

---

## Future Roadmap

### Short-Term (Next Sprint)

1. ~~**Add Glue Crawler** to auto-discover Parquet schema after each job run~~ ✅ **DONE** — Implemented in [`main-stack.yaml`](cloudformation/main-stack.yaml) and [`csv_etl_job.py`](glue-jobs/csv_etl_job.py)
2. **Increase Lambda BatchSize** from 1 to 10 for better throughput
3. **Add CloudWatch Alarms** for Glue job duration and failure rate
4. **Add SNS notifications** for DLQ messages and Glue job failures

### Medium-Term (Next Quarter)

1. **Step Functions orchestration** for Glue job status tracking and retry logic
2. **Schema validation Lambda** before Glue processing
3. **Data quality checks** using AWS Glue Data Quality (or Great Expectations)
4. **Cross-account Lake Formation sharing** for multi-account data mesh

### Long-Term (Next 6 Months)

1. **Apache Iceberg table format** for ACID transactions, time travel, and schema evolution
2. **Real-time ingestion path** using Kinesis → Glue Streaming
3. **Data lineage** using AWS Glue Data Catalog lineage or OpenLineage
4. **Multi-region disaster recovery** with S3 Cross-Region Replication

---

## Appendix

### File Reference

| File | Purpose |
|------|---------|
| [`lambda/glue_trigger.py`](lambda/glue_trigger.py) | Lambda: SQS → Glue trigger with S3 conditional put locking |
| [`glue-jobs/csv_etl_job.py`](glue-jobs/csv_etl_job.py) | Glue PySpark job: CSV → Parquet ETL with idempotency |
| [`cloudformation/main-stack.yaml`](cloudformation/main-stack.yaml) | CloudFormation: all infrastructure