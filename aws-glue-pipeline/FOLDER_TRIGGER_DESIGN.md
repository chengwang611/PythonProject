# Folder-Level Completion Trigger — Design Document

## Problem Statement

The current Glue ETL pipeline triggers on **every individual `.csv` file** via S3 event notification → SQS → Lambda. However, many data ingestion workflows require waiting until **all files in a specific folder** have been delivered before starting processing.

**Example scenarios:**
- A daily batch where 50 CSV files are uploaded to `s3://bucket/incoming/2026-07-10/`
- A partner data drop where `customers.csv`, `orders.csv`, `inventory.csv` must all be present
- A multi-part upload where files arrive over several minutes

**Core challenge**: S3 has no native "folder upload complete" signal. This must be implemented externally.

---

## Solution A: Sentinel File (Recommended ✅)

The **sentinel file** approach uses a special marker file (e.g., `_COMPLETE`) that is uploaded **after** all data files have been delivered. This is the simplest and most reliable pattern.

### Architecture

```
                          ┌─────────────────────────────────────────────────┐
                          │              UPSTREAM DATA PRODUCER              │
                          │                                                 │
                          │  1. Upload data files:                          │
                          │     s3://bucket/incoming/2026-07-10/part-01.csv │
                          │     s3://bucket/incoming/2026-07-10/part-02.csv │
                          │     s3://bucket/incoming/2026-07-10/part-NN.csv │
                          │                                                 │
                          │  2. Upload sentinel file (LAST):                │
                          │     s3://bucket/incoming/2026-07-10/_COMPLETE   │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 │ S3 Event (suffix: _COMPLETE)
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │              SQS QUEUE (filtered)                │
                          │  Only triggers on *_COMPLETE objects            │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │              LAMBDA (Folder Processor)           │
                          │                                                 │
                          │  1. Parse sentinel path → extract folder prefix │
                          │     s3://bucket/incoming/2026-07-10/_COMPLETE   │
                          │     → folder = incoming/2026-07-10/             │
                          │                                                 │
                          │  2. List all .csv files in that folder          │
                          │     s3.list_objects_v2(Prefix=folder)           │
                          │                                                 │
                          │  3. Acquire folder-level lock                   │
                          │     put_object(_locks/{folder_hash}.lock,       │
                          │                IfNoneMatch='*')                 │
                          │                                                 │
                          │  4. Start Glue job with ALL file paths          │
                          │     glue.start_job_run(                         │
                          │       --input_paths=[file1,file2,...]           │
                          │       --folder_prefix=incoming/2026-07-10/      │
                          │     )                                           │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 ▼
                          ┌─────────────────────────────────────────────────┐
                          │              GLUE JOB (Folder ETL)               │
                          │                                                 │
                          │  1. Read all CSV files in the folder            │
                          │     spark.read.csv(folder_path + "*.csv")       │
                          │                                                 │
                          │  2. Union/merge all DataFrames                  │
                          │                                                 │
                          │  3. Apply transformations                       │
                          │                                                 │
                          │  4. Write single Parquet output                 │
                          └─────────────────────────────────────────────────┘
```

### Step 1: S3 Event Notification Configuration

Change the S3 notification filter from `.csv` suffix to `_COMPLETE` suffix in [`main-stack.yaml`](cloudformation/main-stack.yaml):

```yaml
DataBucket:
  Type: AWS::S3::Bucket
  Properties:
    NotificationConfiguration:
      QueueConfigurations:
        - Event: s3:ObjectCreated:Put
          Filter:
            S3Key:
              Rules:
                - Name: suffix
                  Value: _COMPLETE    # ← Changed from .csv
          Queue: !GetAtt IngestionQueue.Arn
```

### Step 2: Lambda Changes — Folder-Level Processing

Add these functions to [`glue_trigger.py`](lambda/glue_trigger.py):

```python
def _extract_folder_from_sentinel(s3_key: str) -> str:
    """
    Extract the folder prefix from a sentinel file path.
    
    Example:
        incoming/2026-07-10/_COMPLETE → incoming/2026-07-10/
        data/daily/_COMPLETE → data/daily/
    """
    folder = s3_key.rsplit('/', 1)[0] + '/'
    return folder


def _list_csv_files_in_folder(bucket: str, folder_prefix: str) -> list[str]:
    """
    List all .csv files in the given S3 folder prefix.
    Returns full s3:// URIs.
    """
    csv_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                csv_files.append(f"s3://{bucket}/{obj['Key']}")
    
    logger.info("Found %d CSV files in folder %s", len(csv_files), folder_prefix)
    return csv_files


def _start_glue_job_for_folder(folder_prefix: str, csv_files: list[str], 
                                s3_bucket: str) -> str:
    """
    Start a Glue job to process all CSV files in a folder.
    Passes the list of files as a JSON array argument.
    """
    job_id = uuid.uuid4().hex[:12]
    folder_hash = hashlib.sha256(folder_prefix.encode()).hexdigest()
    output_path = f"s3://{PROCESSED_BUCKET}/output/{folder_hash}/{job_id}/"
    
    arguments = {
        '--input_paths': json.dumps(csv_files),      # JSON array of all files
        '--folder_prefix': folder_prefix,
        '--output_path': output_path,
        '--job_id': job_id,
        '--environment': ENVIRONMENT,
        '--file_dedup_key': folder_hash,              # Dedup at folder level
    }
    
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments=arguments,
    )
    return response['JobRunId']
```

**Modified `lambda_handler` logic**:

```python
def lambda_handler(event: dict, context) -> dict:
    # ... existing setup ...
    
    for record in event.get('Records', []):
        # ... parse S3 event ...
        
        for s3_record in s3_records:
            bucket = s3_record['bucket']
            key = s3_record['key']
            
            # NEW: Extract folder from sentinel file
            folder_prefix = _extract_folder_from_sentinel(key)
            
            # NEW: Folder-level lock (instead of file-level)
            if not _try_acquire_lock(bucket, folder_prefix):  # Lock on folder, not file
                skipped_count += 1
                continue
            
            try:
                # NEW: List all CSV files in the folder
                csv_files = _list_csv_files_in_folder(bucket, folder_prefix)
                
                if not csv_files:
                    logger.warning("No CSV files found in folder %s", folder_prefix)
                    _release_lock(bucket, folder_prefix)
                    continue
                
                # NEW: Start Glue job with all files
                run_id = _start_glue_job_for_folder(
                    folder_prefix=folder_prefix,
                    csv_files=csv_files,
                    s3_bucket=bucket,
                )
                processed_count += 1
                
            except Exception as exc:
                _release_lock(bucket, folder_prefix)
                # ... error handling ...
```

### Step 3: Glue Job Changes — Multi-File Input

Add to [`csv_etl_job.py`](glue-jobs/csv_etl_job.py):

```python
def read_csv_files(spark: SparkSession, input_paths_json: str, 
                   delimiter: str, header: bool, infer_schema: bool):
    """
    Read multiple CSV files from a JSON array of S3 paths.
    Unions all files into a single DataFrame using unionByName
    (handles column order differences).
    """
    import json
    from functools import reduce
    from pyspark.sql import DataFrame
    
    paths = json.loads(input_paths_json)
    logger.info("Reading %d CSV files from folder", len(paths))
    
    dfs = []
    for path in paths:
        df = spark.read \
            .format('csv') \
            .option('delimiter', delimiter) \
            .option('header', str(header).lower()) \
            .option('inferSchema', str(infer_schema).lower()) \
            .option('mode', 'PERMISSIVE') \
            .load(path)
        dfs.append(df)
    
    if len(dfs) == 1:
        combined = dfs[0]
    else:
        combined = reduce(DataFrame.unionByName, dfs)
    
    logger.info("Combined %d files: %d records", len(paths), combined.count())
    return combined
```

**Modified `main()` logic**:

```python
def main():
    args = parse_args()
    # ...
    
    # NEW: Check if we're in folder mode (multiple files)
    input_paths_json = get_arg(args, 'input_paths', '')
    
    if input_paths_json:
        # Folder mode: read multiple files
        df = read_csv_files(
            spark=spark,
            input_paths_json=input_paths_json,
            delimiter=delimiter,
            header=header.lower() == 'true',
            infer_schema=infer_schema.lower() == 'true',
        )
    else:
        # Single file mode (backward compatible)
        input_path = get_arg(args, 'input_path', '')
        df = read_csv(spark, input_path, delimiter, 
                      header.lower() == 'true', infer_schema.lower() == 'true')
    
    # ... rest of transform + write logic unchanged ...
```

### Sentinel File Content (Optional Enhancement)

The `_COMPLETE` file can carry metadata for validation:

```json
{
  "folder": "incoming/2026-07-10/",
  "timestamp": "2026-07-10T08:00:00Z",
  "expected_file_count": 50,
  "expected_total_size_bytes": 1048576000,
  "source_system": "ERP-Daily-Export",
  "checksum_sha256": "abc123...",
  "files": [
    {"name": "part-01.csv", "size": 20971520, "rows": 100000},
    {"name": "part-02.csv", "size": 20971520, "rows": 100000}
  ]
}
```

The Lambda can then validate:
- Actual file count vs `expected_file_count`
- Total size vs `expected_total_size_bytes`
- Individual file checksums

If validation fails → send to DLQ with detailed error, do NOT start Glue job.

### Pros & Cons

| Pros | Cons |
|------|------|
| Simplest to implement and debug | Requires upstream to upload sentinel file |
| Explicit, unambiguous "done" signal | Upstream must ensure sentinel is uploaded LAST |
| Works regardless of file count, naming, or upload order | If upstream crashes before sentinel, folder is never processed |
| `_COMPLETE` can carry metadata for validation | |
| Minimal changes to existing pipeline | |
| No new AWS infrastructure needed | |

---

## Solution B: DynamoDB File Counter

Instead of a sentinel file, use DynamoDB to track expected file counts.

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  How it works:                                                    │
│                                                                   │
│  1. Upstream system registers expected file count in DynamoDB:    │
│     { folder: "2026-07-10/", expected_count: 50, arrived: 0 }    │
│                                                                   │
│  2. Each .csv S3 event triggers Lambda → increments arrived count │
│                                                                   │
│  3. When arrived == expected_count → trigger Glue job             │
│                                                                   │
│  4. Use DynamoDB conditional update for atomicity:                │
│     UPDATE SET arrived = arrived + 1                              │
│     WHERE folder = :folder AND arrived < expected_count           │
│     RETURNING ALL_NEW                                             │
│                                                                   │
│  5. If returned item.arrived == item.expected_count → trigger     │
└──────────────────────────────────────────────────────────────────┘
```

### DynamoDB Table Schema

```yaml
TableName: folder-ingestion-tracker
PrimaryKey: folder_prefix (String)
Attributes:
  - folder_prefix: "incoming/2026-07-10/"
  - expected_count: 50
  - arrived_count: 0
  - status: "PENDING" | "PROCESSING" | "COMPLETED"
  - first_file_at: "2026-07-10T08:00:00Z"
  - last_file_at: "2026-07-10T08:05:00Z"
  - ttl: 1720569600  # Auto-delete after 30 days
```

### Lambda Counter Logic

```python
def _increment_and_check(bucket: str, folder_prefix: str) -> bool:
    """
    Atomically increment the arrived count for a folder.
    Returns True if this was the last expected file (trigger processing).
    """
    response = dynamodb.update_item(
        TableName=COUNTER_TABLE,
        Key={'folder_prefix': {'S': folder_prefix}},
        UpdateExpression='SET arrived_count = arrived_count + :inc, last_file_at = :now',
        ConditionExpression='arrived_count < expected_count AND attribute_exists(folder_prefix)',
        ExpressionAttributeValues={
            ':inc': {'N': '1'},
            ':now': {'S': datetime.now(timezone.utc).isoformat()},
        },
        ReturnValues='ALL_NEW',
    )
    
    item = response['Attributes']
    arrived = int(item['arrived_count']['N'])
    expected = int(item['expected_count']['N'])
    
    return arrived >= expected
```

### Pros & Cons

| Pros | Cons |
|------|------|
| No sentinel file needed | Requires upstream to know exact file count |
| Real-time progress tracking | DynamoDB cost (~$1.25/million writes) |
| Works with streaming uploads | Race conditions on concurrent uploads |
| Atomic counter via conditional updates | Complex error handling (what if count is wrong?) |
| | Additional infrastructure to manage |

---

## Solution C: Time Window (EventBridge Scheduler)

Wait for a configurable time window after the last file arrives.

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  How it works:                                                    │
│                                                                   │
│  1. Each .csv S3 event triggers Lambda                            │
│                                                                   │
│  2. Lambda writes a "heartbeat" to DynamoDB with TTL              │
│     { folder: "2026-07-10/", last_file_at: timestamp }           │
│                                                                   │
│  3. EventBridge Scheduler runs every N minutes (e.g., every 5)    │
│     → Queries DynamoDB for folders where:                         │
│       last_file_at < now - window_threshold (e.g., 10 min)        │
│       AND status != "PROCESSED"                                   │
│     → Triggers Glue job for each eligible folder                  │
│                                                                   │
│  4. Mark folder as "PROCESSED" in DynamoDB                        │
└──────────────────────────────────────────────────────────────────┘
```

### EventBridge Scheduler Lambda

```python
def check_stale_folders(event, context):
    """
    EventBridge scheduled Lambda — checks for folders that have
    stopped receiving files and triggers processing.
    """
    window_minutes = int(os.environ.get('IDLE_WINDOW_MINUTES', '10'))
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    
    # Scan DynamoDB for folders past the idle window
    response = dynamodb.scan(
        TableName=HEARTBEAT_TABLE,
        FilterExpression='last_file_at < :cutoff AND #status = :pending',
        ExpressionAttributeValues={
            ':cutoff': {'S': cutoff.isoformat()},
            ':pending': {'S': 'PENDING'},
        },
        ExpressionAttributeNames={'#status': 'status'},
    )
    
    for item in response['Items']:
        folder = item['folder_prefix']['S']
        # Mark as processing to prevent duplicate triggers
        dynamodb.update_item(
            TableName=HEARTBEAT_TABLE,
            Key={'folder_prefix': {'S': folder}},
            UpdateExpression='SET #status = :processing',
            ExpressionAttributeValues={':processing': {'S': 'PROCESSING'}},
            ExpressionAttributeNames={'#status': 'status'},
        )
        # Trigger Glue job for this folder
        trigger_glue_for_folder(folder)
```

### Pros & Cons

| Pros | Cons |
|------|------|
| No sentinel file needed | Adds latency (wait for idle window) |
| No upstream changes required | Tuning the window is tricky |
| Handles variable file counts | EventBridge Scheduler + DynamoDB cost |
| Works with any upload pattern | May trigger prematurely for slow uploads |
| | May miss folders if uploads are very slow |

---

## Solution D: S3 Batch Operations + Manifest

Use an S3 Batch Operations job with a manifest file.

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  How it works:                                                    │
│                                                                   │
│  1. Upstream creates a manifest CSV listing all files:            │
│     bucket,key                                                    │
│     my-bucket,incoming/2026-07-10/part-01.csv                    │
│     my-bucket,incoming/2026-07-10/part-02.csv                    │
│                                                                   │
│  2. Manifest upload triggers Lambda via S3 event                  │
│     (filtered on suffix: manifest.json or _MANIFEST.csv)          │
│                                                                   │
│  3. Lambda reads manifest → extracts file list                    │
│                                                                   │
│  4. Lambda starts Glue job with the file list from manifest       │
└──────────────────────────────────────────────────────────────────┘
```

### Manifest Format

```csv
bucket,key,size,checksum
my-data-bucket,incoming/2026-07-10/part-01.csv,20971520,sha256:abc123
my-data-bucket,incoming/2026-07-10/part-02.csv,20971520,sha256:def456
```

### Pros & Cons

| Pros | Cons |
|------|------|
| Explicit file list (no discovery needed) | Upstream must generate manifest |
| Can include metadata (row counts, checksums) | Manifest format must be agreed upon |
| Works with any file types (not just CSV) | Extra file to manage |
| No S3 LIST operations needed | Manifest could be out of sync with actual files |

---

## Recommendation Matrix

| Scenario | Recommended Approach | Why |
|----------|---------------------|-----|
| **Upstream can be modified** | **Sentinel File** (`_COMPLETE`) | Simplest, most reliable, no new infrastructure |
| **Upstream cannot be modified, known file count** | DynamoDB Counter | No upstream changes needed |
| **Upstream cannot be modified, unknown file count** | Time Window (EventBridge) | Handles arbitrary upload patterns |
| **Need file-level metadata/validation** | Manifest File | Carries checksums, row counts, etc. |
| **Streaming/continuous uploads** | Time Window (EventBridge) | Natural fit for windowed processing |

### Implementation Effort Comparison

| Approach | S3 Config | Lambda Change | Glue Job Change | New Infrastructure | Total Effort |
|----------|-----------|---------------|-----------------|-------------------|-------------|
| **Sentinel File** | Suffix: `_COMPLETE` | Add folder listing + multi-file trigger | Add multi-file read + union | None | **Low** |
| DynamoDB Counter | None | Add counter logic | None | DynamoDB table | Medium |
| Time Window | None | Add heartbeat logic | None | DynamoDB + EventBridge Scheduler | Medium-High |
| Manifest File | Suffix: `manifest.json` | Add manifest parsing | Add multi-file read | None | Low-Medium |

---

## Edge Cases & Failure Handling

### Sentinel File Approach

| Edge Case | Handling |
|-----------|----------|
| **Sentinel uploaded before all data files** | Lambda lists folder → finds fewer files than expected → if `_COMPLETE` has `expected_file_count`, validation fails → DLQ |
| **Upstream crashes before sentinel** | Folder never processed → needs manual intervention or timeout-based fallback (combine with Time Window approach) |
| **Duplicate sentinel upload** | Folder-level S3 lock (`IfNoneMatch='*'`) prevents duplicate processing |
| **Empty folder (no CSV files)** | Lambda lists folder → finds 0 files → logs warning, releases lock, skips |
| **Mixed file types in folder** | Lambda filters only `.csv` files; non-CSV files are ignored |
| **Very large folder (10,000+ files)** | S3 ListObjectsV2 pagination handles this; Glue job may need more workers |

### DynamoDB Counter Approach

| Edge Case | Handling |
|-----------|----------|
| **Expected count is wrong (too high)** | Folder never triggers → needs timeout + alert |
| **Expected count is wrong (too low)** | Extra files are ignored (only first N processed) |
| **Concurrent Lambda invocations** | DynamoDB conditional update ensures atomic counter |
| **DynamoDB throttling** | Exponential backoff in Lambda; provisioned capacity or on-demand mode |

### Time Window Approach

| Edge Case | Handling |
|-----------|----------|
| **Window too short** | Triggers before all files arrive → partial data processed |
| **Window too long** | Unnecessary delay before processing |
| **Slow but ongoing upload** | Each new file resets the `last_file_at` timestamp → window restarts |
| **EventBridge misses a cycle** | Next cycle catches it (idempotent via `status` field) |

---

## Migration Path: File-Level → Folder-Level

If you want to support **both** individual file triggers AND folder-level triggers simultaneously:

```yaml
# CloudFormation: Two SQS queues, two Lambda triggers
DataBucket:
  Type: AWS::S3::Bucket
  Properties:
    NotificationConfiguration:
      QueueConfigurations:
        # Queue 1: Individual file processing (existing)
        - Event: s3:ObjectCreated:Put
          Filter:
            S3Key:
              Rules:
                - Name: suffix
                  Value: .csv
          Queue: !GetAtt FileIngestionQueue.Arn
        
        # Queue 2: Folder completion trigger (new)
        - Event: s3:ObjectCreated:Put
          Filter:
            S3Key:
              Rules:
                - Name: suffix
                  Value: _COMPLETE
          Queue: !GetAtt FolderIngestionQueue.Arn
```

This allows gradual migration: some folders use sentinel files, others continue with per-file processing.

---

## Appendix: Related Files

| File | Purpose |
|------|---------|
| [`lambda/glue_trigger.py`](lambda/glue_trigger.py) | Lambda: SQS → Glue trigger (needs folder-listing logic) |
| [`glue-jobs/csv_etl_job.py`](glue-jobs/csv_etl_job.py) | Glue PySpark job (needs multi-file read support) |
| [`cloudformation/main-stack.yaml`](cloudformation/main-stack.yaml) | CloudFormation (needs S3 suffix filter change) |
| [`DESIGN.md`](DESIGN.md) | Main pipeline design document |
