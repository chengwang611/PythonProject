# AWS Glue ETL Pipeline - S3 → SQS → Lambda → Glue (PySpark)

A **serverless** event-driven ETL pipeline that automatically processes CSV files dropped into an S3 bucket using **AWS Glue** (instead of EMR). Designed for **exactly-once processing** using S3 conditional put (`IfNoneMatch='*'`) — no DynamoDB needed.

## Why Glue Instead of EMR?

| Aspect | EMR | Glue (this pipeline) |
|--------|-----|---------------------|
| **Cluster startup** | 5-10 minutes | **1-2 minutes** |
| **Pricing** | Pay for EC2 instances per hour | **Pay per DPU-second** (no idle cost) |
| **Management** | You manage EC2/EMR | **Fully serverless** |
| **Scaling** | Manual instance count | **Automatic** |
| **VPC** | Required | **Optional** |
| **Cost for 1000 files** | ~$50-200 | **~$5-20** |

## Architecture

```
┌──────────────┐     S3 Event     ┌──────────────┐     SQS Message     ┌──────────────────────┐
│   S3 Bucket  │ ───────────────► │  SQS Queue   │ ──────────────────► │    Lambda            │
│  (CSV Drop)  │                  │ (Ingestion)  │                     │  (S3 Conditional Put) │
└──────────────┘                  └──────────────┘                     └──────────┬───────────┘
                                                                                  │
                                                                            ┌─────▼──────┐
                                                                            │  S3 Bucket │
                                                                            │  (_locks/) │
                                                                            └─────┬──────┘
                                                                                  │
                                                                      ┌───────────▼───────────┐
                                                                      │  IfNoneMatch='*'      │
                                                                      │  (Atomic Check-Set)   │
                                                                      └───────────┬───────────┘
                                                                                  │
                                                                     ┌────────────▼────────────┐
                                                                     │  Glue: StartJobRun      │
                                                                     │  (Serverless Spark)     │
                                                                     └────────────┬────────────┘
                                                                                  │
                                                                          ┌───────▼────────┐
                                                                          │  S3 Processed   │
                                                                          │  (Parquet Out)  │
                                                                          └────────────────┘
```

### Data Flow

1. **CSV file lands** in the S3 data bucket
2. **S3 event notification** sends a message to the SQS ingestion queue (filtered for `.csv` suffix)
3. **Lambda function** (SQS-triggered) reads the message and:
   - **Exactly-Once Check**: S3 conditional put with `IfNoneMatch='*'` — atomic distributed lock
     - If lock exists → skip (already processed)
     - If lock is new → acquire and proceed
   - **Starts Glue job** via `glue.start_job_run()` — no cluster to create
   - On failure → releases the S3 lock so the file can be retried
4. **Glue job** (serverless Spark) runs the PySpark ETL which:
   - **Idempotency Check**: Checks for existing S3 marker before processing
   - Reads CSV from S3
   - Applies transformations (clean, aggregate, or passthrough)
   - Writes Parquet output to processed S3 bucket
   - Writes `_SUCCESS` dedup marker and `_MANIFEST.json`
5. **SQS message is deleted** after successful processing
6. **Failed messages** go to a Dead Letter Queue (DLQ)

## Project Structure

```
aws-glue-pipeline/
├── .github/workflows/
│   └── deploy-glue-pipeline.yml     # GitHub Actions CI/CD
├── cloudformation/
│   └── main-stack.yaml              # Main CloudFormation stack
├── glue-jobs/
│   └── csv_etl_job.py               # Glue PySpark ETL job
├── lambda/
│   ├── glue_trigger.py              # Lambda function (SQS → Glue)
│   └── requirements.txt             # Lambda Python dependencies
├── scripts/
│   └── deploy.sh                    # One-command deployment script
├── config.example.yaml              # Example configuration
├── Makefile                         # Local development commands
└── README.md                        # This file
```

## Prerequisites

- **AWS CLI** installed and configured (`aws configure`)
- **Python 3.12+** with `pip`
- **AWS Account** with permissions to create:
  - S3 buckets, SQS queues, Lambda functions, Glue jobs
  - IAM roles and policies
  - CloudFormation stacks

## Build and Deploy on AWS

### Step 1: Clone and configure

```bash
cd aws-glue-pipeline
cp config.example.yaml config.dev.yaml
# Edit config.dev.yaml with your values
```

### Step 2: Build the Lambda package

```bash
make package-lambda ENVIRONMENT=dev
# Or manually:
cd lambda
pip install -r requirements.txt -t ./package
cp glue_trigger.py ./package/
cd package && zip -r9 ../glue-trigger-dev.zip . && cd ..
rm -rf package
cd ..
```

### Step 3: Create artifact buckets and upload

```bash
export AWS_REGION=us-east-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create buckets
for bucket in "my-glue-lambda-artifacts" "my-glue-artifacts" "my-glue-cfn-templates"; do
  aws s3 mb "s3://${bucket}" --region "$AWS_REGION" 2>/dev/null || true
done

# Upload artifacts
aws s3 cp lambda/glue-trigger-dev.zip s3://my-glue-lambda-artifacts/lambda/glue-trigger-dev.zip
aws s3 cp glue-jobs/csv_etl_job.py s3://my-glue-artifacts/glue-jobs/csv_etl_job.py
aws s3 cp cloudformation/main-stack.yaml s3://my-glue-cfn-templates/glue-pipeline/main-stack.yaml
```

### Step 4: Deploy CloudFormation

```bash
aws cloudformation deploy \
  --template-file cloudformation/main-stack.yaml \
  --stack-name glue-etl-pipeline-dev \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
  --parameter-overrides \
    EnvironmentName=dev \
    LambdaS3Bucket=my-glue-lambda-artifacts \
    GlueArtifactsBucket=my-glue-artifacts \
    DataBucketName=my-glue-data \
    ProcessedBucketName=my-glue-processed \
    GithubRepoOwner=your-github-username \
    GithubRepoName=your-repo-name
```

### Step 5: Test the pipeline

```bash
# Get bucket names from stack outputs
DATA_BUCKET=$(aws cloudformation describe-stacks --stack-name glue-etl-pipeline-dev \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)

# Upload a test CSV
echo "name,age,city\nAlice,30,NYC\nBob,25,SF" > /tmp/test.csv
aws s3 cp /tmp/test.csv "s3://${DATA_BUCKET}/incoming/test.csv"

# Watch Lambda logs
aws logs tail /aws/lambda/dev-glue-trigger --follow

# Check Glue job runs
aws glue get-job-runs --job-name dev-csv-etl-job --query "JobRuns[0:3].[Id,JobRunState]" --output table
```

## Alternative: One-Command Deployment

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev
```

## Alternative: Deploy with Make

```bash
export AWS_REGION=us-east-1
export LAMBDA_BUCKET=my-glue-lambda-artifacts
export GLUE_ARTIFACTS_BUCKET=my-glue-artifacts
export DATA_BUCKET_NAME=my-glue-data
export PROCESSED_BUCKET_NAME=my-glue-processed
export GITHUB_OWNER=your-github-username
export GITHUB_REPO=your-repo-name

make deploy ENVIRONMENT=dev
```

## GitHub Actions CI/CD

### Set up GitHub OIDC

1. Deploy the stack once manually (see above)
2. The stack creates an IAM role `{env}-github-deploy-role` for GitHub OIDC
3. In your GitHub repository, add these **secrets**:

| Secret | Description |
|--------|-------------|
| `DEPLOY_ROLE_ARN` | ARN of the GitHub deploy role (from stack outputs) |
| `LAMBDA_ARTIFACTS_BUCKET` | S3 bucket for Lambda ZIPs |
| `GLUE_ARTIFACTS_BUCKET` | S3 bucket for Glue job scripts |
| `DATA_BUCKET_NAME` | Name prefix for data bucket |
| `PROCESSED_BUCKET_NAME` | Name prefix for processed bucket |
| `CFN_TEMPLATES_BUCKET` | S3 bucket for CloudFormation templates |

Push to `main` to trigger automated validation, packaging, and deployment.

## CloudFormation Stack Details

The stack creates:

| Resource | Description |
|----------|-------------|
| **S3 Buckets** | Data (CSV input), Processed (Parquet output + locks), Artifacts (Glue scripts) |
| **SQS Queue** | Ingestion queue with DLQ for failed messages |
| **Lambda Function** | SQS consumer that triggers Glue jobs |
| **Glue Job** | Serverless PySpark ETL job |
| **IAM Roles** | Lambda execution role, Glue job role, GitHub OIDC deploy role |
| **CloudWatch Logs** | Lambda log group with configurable retention |

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `EnvironmentName` | `dev` | Environment (dev/staging/prod) |
| `GlueWorkerType` | `G.1X` | Glue worker type (G.1X = 16GB) |
| `GlueWorkerCount` | `2` | Number of Glue workers |
| `GlueJobTimeout` | `60` | Glue job timeout (minutes) |
| `LambdaMemorySize` | `256` | Lambda memory (MB) |
| `LambdaTimeout` | `120` | Lambda timeout (seconds) |

## Lambda Function Details

The Lambda ([`glue_trigger.py`](aws-glue-pipeline/lambda/glue_trigger.py)) is triggered by SQS messages containing S3 event notifications.

### Key Behavior

- **S3 conditional put** (`IfNoneMatch='*'`) for distributed locking — no DynamoDB
- Calls **`glue.start_job_run()`** instead of creating EMR clusters
- Lock objects stored at `s3://{processed-bucket}/_locks/{hash}.lock`
- On failure → releases lock so the file can be retried
- On S3 unavailability → falls through to at-least-once

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ENVIRONMENT` | Environment name |
| `GLUE_JOB_NAME` | Glue job name to trigger |
| `GLUE_ARTIFACTS_BUCKET` | Bucket with Glue scripts |
| `PROCESSED_BUCKET` | Output bucket |
| `DATA_BUCKET` | Input bucket |
| `LOCK_BUCKET` | Bucket for distributed locks |
| `LOCK_EXPIRY_DAYS` | Lock retention (default: 7) |

## Glue Job Details

The Glue job ([`csv_etl_job.py`](aws-glue-pipeline/glue-jobs/csv_etl_job.py)) runs serverless Spark on AWS Glue.

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--input_path` | (required) | S3 path to input CSV |
| `--output_path` | (required) | S3 path for output |
| `--job_id` | (required) | Unique job identifier |
| `--environment` | `dev` | Environment name |
| `--file_dedup_key` | (optional) | SHA-256 hash for dedup |
| `--delimiter` | `,` | CSV delimiter |
| `--header` | `true` | CSV has header |
| `--infer_schema` | `true` | Infer column types |
| `--partition_by` | (none) | Column(s) to partition output |
| `--num_partitions` | `1` | Output partitions |
| `--transform` | `clean` | Transform type |
| `--aggregate_column` | (none) | Column to aggregate |
| `--aggregate_func` | `sum` | Aggregation function |
| `--drop_columns` | (none) | Columns to drop |
| `--rename_columns` | `{}` | Column rename map (JSON) |
| `--date_column` | (none) | Column to parse as date |

### Transform Types

- **`clean`** (default): Drops corrupt records, null rows, duplicates; renames/drops columns; parses dates
- **`aggregate`**: Groups by non-numeric columns and applies aggregation
- **`passthrough`**: No transformation, writes data as-is

### Output

- **Parquet format** with Snappy compression
- **`_MANIFEST.json`** with job metrics
- **`_dedup_markers/{hash}/_SUCCESS`** for idempotency
- **Partitioned** by specified columns (optional)

## Exactly-Once Processing

The pipeline uses a **two-layer defense** with no DynamoDB:

### Layer 1: S3 Conditional Put (Lambda)

```python
s3_client.put_object(
    Bucket=lock_bucket,
    Key=f"_locks/{file_hash}.lock",
    IfNoneMatch='*',  # Atomic: only succeed if object does NOT exist
)
```

- **First invocation**: `put_object` succeeds → lock acquired → process file
- **Second invocation**: `PreconditionFailed` → file already being processed → skip
- **Lambda crash after Glue submit**: S3 lock exists → next retry skips
- **S3 unavailable**: Falls through to at-least-once

### Layer 2: S3 Marker (Glue Job)

```python
marker_path = f"{output_path}/_dedup_markers/{file_dedup_key}/_SUCCESS"
if fs.exists(marker_path):
    logger.warning("File already processed. Exiting early.")
```

### Failure Mode Analysis

| Failure Scenario | S3 Lock State | Result |
|-----------------|---------------|--------|
| Lambda crashes before S3 lock | No lock | **Retried** ✅ |
| Lambda crashes after S3 lock, before Glue start | Lock exists | **Exactly once** ✅ |
| Lambda crashes after Glue start, before SQS delete | Lock exists | **Exactly once** ✅ |
| Glue job fails | Lock released by Lambda | **Retried** ✅ |
| Same file uploaded twice | Lock from first upload exists | **Exactly once** ✅ |
| S3 duplicate event | Lock from first event exists | **Exactly once** ✅ |

## Monitoring

```bash
# Lambda logs
aws logs tail /aws/lambda/dev-glue-trigger --follow

# Glue job runs
aws glue get-job-runs --job-name dev-csv-etl-job --query "JobRuns[0:5].[Id,JobRunState]" --output table

# Active locks
aws s3 ls s3://my-glue-processed-{account}-dev/_locks/

# Processed output
aws s3 ls s3://my-glue-processed-{account}-dev/output/ --recursive
```

## Clean Up

```bash
# Empty processed bucket (contains locks)
aws s3 rm "s3://${PROCESSED_BUCKET}" --recursive

# Delete stack
aws cloudformation delete-stack --stack-name glue-etl-pipeline-dev
aws cloudformation wait stack-delete-complete --stack-name glue-etl-pipeline-dev
```

## Cost Comparison: Glue vs EMR

| Resource | EMR Pipeline | Glue Pipeline |
|----------|-------------|---------------|
| **Compute** | 2x m5.xlarge @ $0.192/hr + EMR premium | G.1X workers @ $0.44/DPU-hour |
| **Startup time** | 5-10 min (billed) | 1-2 min (billed) |
| **Idle cost** | $0.192/hr if cluster left running | **$0** — no idle |
| **Per-file cost (est.)** | ~$0.05-0.20 | **~$0.005-0.02** |
| **Management overhead** | High (EC2, EMR, security groups) | **Low** (serverless) |

## License

MIT
