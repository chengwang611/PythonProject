# AWS ETL Pipeline - S3 → SQS → Lambda → EMR (PySpark)

An event-driven ETL pipeline that automatically processes CSV files dropped into an S3 bucket using Amazon EMR with PySpark. Designed for **exactly-once processing** using S3 conditional put (IfNoneMatch='*') — no DynamoDB needed.

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
                                                                     │  Create/Submit EMR Step │
                                                                     └────────────┬────────────┘
                                                                                  │
                                                                        ┌─────────▼─────────┐
                                                                        │   EMR Cluster     │
                                                                        │  (PySpark Job)    │
                                                                        │  (Idempotent)     │
                                                                        └─────────┬─────────┘
                                                                                  │
                                                                          ┌───────▼────────┐
                                                                          │  S3 Processed   │
                                                                          │  (Parquet Out)  │
                                                                          └────────────────┘
```

### Data Flow

1. **CSV file lands** in the S3 data bucket (e.g., `s3://my-data-bucket/incoming/report.csv`)
2. **S3 event notification** sends a message to the SQS ingestion queue (filtered for `.csv` suffix)
3. **Lambda function** (SQS-triggered) reads the message, parses the S3 event, and:
   - **Exactly-Once Check**: Performs an S3 conditional put with `IfNoneMatch='*'`
     - If the lock object **already exists** → skips (file already processed or in progress)
     - If the lock object is **new** → acquires distributed lock and proceeds
   - Creates a **transient EMR cluster** (auto-terminates) OR uses a **long-running cluster**
   - Submits a **PySpark step** with the file's dedup hash
   - On failure → releases the S3 lock so the file can be retried
4. **EMR cluster** runs the PySpark job which:
   - **Idempotency Check**: Checks for an existing S3 marker file before processing
   - Reads the CSV from S3
   - Applies transformations (cleaning, aggregation, or passthrough)
   - Writes the output as **Parquet** to the processed S3 bucket
   - Writes a `_SUCCESS` dedup marker and `_MANIFEST.json` with job metrics
5. **SQS message is deleted** only after ALL files in the batch succeed
6. **Failed messages** go to a Dead Letter Queue (DLQ) for investigation

## Project Structure

```
aws-etl-pipeline/
├── .github/workflows/
│   └── deploy-etl-pipeline.yml     # GitHub Actions CI/CD
├── cloudformation/
│   └── main-stack.yaml             # Main CloudFormation stack
├── emr-jobs/
│   └── csv_etl_job.py              # PySpark ETL job for EMR
├── lambda/
│   ├── emr_trigger.py              # Lambda function (SQS → EMR)
│   └── requirements.txt            # Lambda Python dependencies
├── scripts/
│   └── deploy.sh                   # One-command deployment script
├── config.example.yaml             # Example configuration
├── Makefile                        # Local development commands
└── README.md                       # This file
```

## Prerequisites

Before you begin, ensure you have:

- **AWS CLI** installed and configured (`aws configure`)
- **Python 3.12+** with `pip`
- **AWS Account** with permissions to create:
  - S3 buckets, SQS queues, Lambda functions, EMR clusters
  - IAM roles and policies
  - CloudFormation stacks
- **GitHub repository** (for CI/CD deployment via OIDC)

## Build and Deploy on AWS

### Step 1: Clone the repository

```bash
git clone <your-repo-url>
cd aws-etl-pipeline
```

### Step 2: Configure your environment

Create a configuration file for your target environment:

```bash
cp config.example.yaml config.dev.yaml
```

Edit `config.dev.yaml` with your values:

```yaml
environment: dev
aws_region: us-east-1

# S3 Buckets — these are name prefixes; the stack appends -{account}-{env}
data_bucket_name: my-etl-data
processed_bucket_name: my-etl-processed
emr_artifacts_bucket: my-etl-artifacts
emr_log_bucket: my-etl-logs
lambda_artifacts_bucket: my-etl-lambda-artifacts
cfn_templates_bucket: my-etl-cfn-templates

# EMR Configuration
emr_release_label: emr-7.5.0
emr_master_instance_type: m5.xlarge
emr_core_instance_type: m5.xlarge
emr_core_instance_count: 2

# VPC (optional — leave empty to use default VPC)
vpc_id: ""
subnet_id: ""

# GitHub (for OIDC deploy role)
github_repo_owner: your-github-username
github_repo_name: your-repo-name
```

### Step 3: Build the Lambda package

Package the Lambda function into a ZIP file with its dependencies:

```bash
# Method A: Using Make
make package-lambda ENVIRONMENT=dev

# Method B: Manual
cd lambda
pip install -r requirements.txt -t ./package
cp emr_trigger.py ./package/
cd package
zip -r9 ../emr-trigger-dev.zip .
cd ..
rm -rf package
cd ..
```

Verify the package:

```bash
unzip -l lambda/emr-trigger-dev.zip
# Should show: emr_trigger.py, boto3/, urllib3/, etc.
```

### Step 4: Create the artifact S3 buckets

These buckets hold the Lambda ZIP, PySpark script, and CloudFormation template. The CloudFormation stack will create the data/processed/log buckets automatically.

```bash
export AWS_REGION=us-east-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create artifact buckets (only needed once)
for bucket in \
  "my-etl-lambda-artifacts" \
  "my-etl-emr-artifacts" \
  "my-etl-cfn-templates"; do
  if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    aws s3 mb "s3://${bucket}" --region "$AWS_REGION"
    echo "Created bucket: ${bucket}"
  else
    echo "Bucket exists: ${bucket}"
  fi
done
```

### Step 5: Upload artifacts to S3

Upload the Lambda ZIP, PySpark job, and CloudFormation template:

```bash
# Upload Lambda package
aws s3 cp lambda/emr-trigger-dev.zip \
  s3://my-etl-lambda-artifacts/lambda/emr-trigger-dev.zip

# Upload PySpark job
aws s3 cp emr-jobs/csv_etl_job.py \
  s3://my-etl-emr-artifacts/emr-jobs/csv_etl_job.py

# Upload CloudFormation template
aws s3 cp cloudformation/main-stack.yaml \
  s3://my-etl-cfn-templates/etl-pipeline/main-stack.yaml
```

### Step 6: Deploy the CloudFormation stack

Deploy the full infrastructure stack:

```bash
export ENVIRONMENT=dev
export STACK_NAME="etl-pipeline-${ENVIRONMENT}"

aws cloudformation deploy \
  --template-file cloudformation/main-stack.yaml \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
  --parameter-overrides \
    EnvironmentName="${ENVIRONMENT}" \
    LambdaS3Bucket="my-etl-lambda-artifacts" \
    EmrLogBucket="my-etl-emr-logs" \
    EmrArtifactsBucket="my-etl-emr-artifacts" \
    DataBucketName="my-etl-data" \
    ProcessedBucketName="my-etl-processed" \
    GithubRepoOwner="your-github-username" \
    GithubRepoName="your-repo-name" \
    VpcId="" \
    SubnetId="" \
    EmrReleaseLabel="emr-7.5.0" \
    EmrInstanceType="m5.xlarge" \
    EmrCoreInstanceCount="2" \
    LambdaMemorySize="256" \
    LambdaTimeout="120" \
    LogRetentionDays="30" \
    SqsVisibilityTimeout="300" \
    EmrJobTimeout="3600" \
  --tags \
    Environment="${ENVIRONMENT}" \
    Project="ETL-Pipeline" \
    ManagedBy="ManualDeploy"
```

Deployment takes 2-3 minutes. Monitor progress:

```bash
aws cloudformation describe-stack-events \
  --stack-name "${STACK_NAME}" \
  --query "StackEvents[0:10].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId]" \
  --output table
```

### Step 7: Verify the deployment

Once the stack status is `CREATE_COMPLETE`, check the outputs:

```bash
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
```

Expected outputs:

| Output Key | Description |
|------------|-------------|
| `DataBucketName` | S3 bucket for incoming CSV files |
| `ProcessedBucketName` | S3 bucket for processed Parquet output |
| `EmrArtifactsBucketName` | S3 bucket for EMR job scripts |
| `IngestionQueueUrl` | SQS queue URL for CSV events |
| `IngestionQueueArn` | SQS queue ARN |
| `IngestionDeadLetterQueueUrl` | DLQ for failed messages |
| `EmrTriggerLambdaArn` | Lambda function ARN |
| `EmrTriggerLambdaName` | Lambda function name |
| `GithubDeployRoleArn` | IAM role for GitHub Actions |
| `LockBucketName` | S3 bucket for distributed locks |

### Step 8: Test the pipeline end-to-end

```bash
# Get the data bucket name from stack outputs
DATA_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text)

PROCESSED_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='ProcessedBucketName'].OutputValue" \
  --output text)

# Upload a test CSV
echo "name,age,city,amount" > /tmp/test-etl.csv
echo "Alice,30,NYC,100.50" >> /tmp/test-etl.csv
echo "Bob,25,SF,200.75" >> /tmp/test-etl.csv
echo "Charlie,35,NYC,150.25" >> /tmp/test-etl.csv

aws s3 cp /tmp/test-etl.csv "s3://${DATA_BUCKET}/incoming/test-etl.csv"
echo "Uploaded test CSV to s3://${DATA_BUCKET}/incoming/test-etl.csv"

# Watch Lambda logs in real-time
aws logs tail "/aws/lambda/${ENVIRONMENT}-emr-trigger" --follow
# Press Ctrl+C after you see the EMR step submission log
```

### Step 9: Monitor EMR job

```bash
# List EMR clusters
aws emr list-clusters --active

# Get the latest cluster ID
CLUSTER_ID=$(aws emr list-clusters --active \
  --query "Clusters[0].Id" --output text)
echo "Cluster: ${CLUSTER_ID}"

# List steps
aws emr list-steps --cluster-id "${CLUSTER_ID}"

# Check processed output
aws s3 ls "s3://${PROCESSED_BUCKET}/output/" --recursive
```

### Step 10: Verify exactly-once dedup

Upload the same file again to confirm it's skipped:

```bash
aws s3 cp /tmp/test-etl.csv "s3://${DATA_BUCKET}/incoming/test-etl-duplicate.csv"

# Check Lambda logs — should show "S3 lock already exists — skipping duplicate"
aws logs tail "/aws/lambda/${ENVIRONMENT}-emr-trigger" --since 5m

# List active locks
aws s3 ls "s3://${PROCESSED_BUCKET}/_locks/"
```

## Alternative: One-Command Deployment

For convenience, use the deployment script which automates steps 3-7:

```bash
# Make the script executable
chmod +x scripts/deploy.sh

# Deploy to dev
./scripts/deploy.sh dev

# Deploy to staging
./scripts/deploy.sh staging

# Deploy to prod
./scripts/deploy.sh prod
```

## Alternative: Deploy with Make

```bash
# Set required environment variables
export AWS_REGION=us-east-1
export LAMBDA_BUCKET=my-etl-lambda-artifacts
export EMR_ARTIFACTS_BUCKET=my-etl-emr-artifacts
export DATA_BUCKET_NAME=my-etl-data
export PROCESSED_BUCKET_NAME=my-etl-processed
export EMR_LOG_BUCKET=my-etl-emr-logs
export GITHUB_OWNER=your-github-username
export GITHUB_REPO=your-repo-name
export VPC_ID=""
export SUBNET_ID=""

# Deploy everything
make deploy ENVIRONMENT=dev
```

## Clean Up

To destroy all resources created by the stack:

```bash
# Empty the processed bucket first (it contains lock objects)
aws s3 rm "s3://${PROCESSED_BUCKET}" --recursive

# Delete the CloudFormation stack
aws cloudformation delete-stack --stack-name "${STACK_NAME}"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}"

# Optionally delete artifact buckets
aws s3 rb "s3://my-etl-lambda-artifacts" --force
aws s3 rb "s3://my-etl-emr-artifacts" --force
aws s3 rb "s3://my-etl-cfn-templates" --force
```

## GitHub Actions CI/CD

### 1. Set up GitHub OIDC

1. Deploy the stack once manually (see Quick Start above)
2. The stack creates an IAM role `{env}-github-deploy-role` for GitHub OIDC
3. In your GitHub repository, add the following **secrets**:

| Secret | Description |
|--------|-------------|
| `DEPLOY_ROLE_ARN` | ARN of the GitHub deploy role (from stack outputs) |
| `LAMBDA_ARTIFACTS_BUCKET` | S3 bucket for Lambda ZIPs |
| `EMR_ARTIFACTS_BUCKET` | S3 bucket for EMR job scripts |
| `EMR_LOG_BUCKET` | S3 bucket for EMR logs |
| `DATA_BUCKET_NAME` | Name prefix for data bucket |
| `PROCESSED_BUCKET_NAME` | Name prefix for processed bucket |
| `CFN_TEMPLATES_BUCKET` | S3 bucket for CloudFormation templates |
| `VPC_ID` | (Optional) VPC ID for EMR |
| `SUBNET_ID` | (Optional) Subnet ID for EMR |

### 2. Push to deploy

```bash
git add .
git commit -m "Update ETL pipeline"
git push origin main
```

The GitHub Actions workflow will:
1. **Validate** CloudFormation templates and Python syntax
2. **Package** Lambda ZIP and upload to S3
3. **Deploy** CloudFormation stack with the latest artifacts

## CloudFormation Stack Details

The main stack (`cloudformation/main-stack.yaml`) creates:

| Resource | Description |
|----------|-------------|
| **S3 Buckets** | Data (CSV input), Processed (Parquet output), Artifacts (EMR scripts), Logs |
| **SQS Queue** | Ingestion queue with DLQ for failed messages |
| **Lambda Function** | SQS consumer that triggers EMR jobs |
| **IAM Roles** | Lambda execution role, GitHub OIDC deploy role |
| **Security Groups** | (Optional) EMR master/core security groups |
| **CloudWatch Logs** | Lambda log group with configurable retention |

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `EnvironmentName` | `dev` | Environment (dev/staging/prod) |
| `EmrReleaseLabel` | `emr-7.5.0` | EMR release (Spark 3.5) |
| `EmrInstanceType` | `m5.xlarge` | Core node instance type |
| `EmrCoreInstanceCount` | `2` | Number of core nodes |
| `LambdaMemorySize` | `256` | Lambda memory (MB) |
| `LambdaTimeout` | `120` | Lambda timeout (seconds) |
| `SqsVisibilityTimeout` | `300` | SQS visibility timeout |
| `EmrJobTimeout` | `3600` | EMR step timeout |

## Lambda Function Details

The Lambda (`lambda/emr_trigger.py`) is triggered by SQS messages containing S3 event notifications.

### Behavior

- **Transient clusters**: By default, creates a new EMR cluster for each batch of files. The cluster auto-terminates after the job completes.
- **Long-running clusters**: Set the `LONG_RUNNING_CLUSTER_ID` environment variable to reuse an existing cluster.
- **Error handling**: Failed messages go to the DLQ after 5 retries.
- **Batch processing**: Processes one SQS message at a time (batch size = 1) to ensure reliable processing.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ENVIRONMENT` | Environment name |
| `EMR_RELEASE_LABEL` | EMR release version |
| `EMR_MASTER_INSTANCE_TYPE` | Master node instance type |
| `EMR_CORE_INSTANCE_TYPE` | Core node instance type |
| `EMR_CORE_INSTANCE_COUNT` | Number of core nodes |
| `EMR_SERVICE_ROLE` | EMR service IAM role |
| `EMR_EC2_ROLE` | EMR EC2 IAM role |
| `EMR_LOG_URI` | S3 path for EMR logs |
| `EMR_ARTIFACTS_BUCKET` | Bucket with PySpark scripts |
| `EMR_JOB_TIMEOUT` | Step timeout in seconds |
| `PROCESSED_BUCKET` | Output bucket |
| `DATA_BUCKET` | Input bucket |
| `SUBNET_ID` | Subnet for EMR (optional) |
| `VPC_ID` | VPC for EMR (optional) |
| `KEY_NAME` | EC2 key pair (optional) |
| `LONG_RUNNING_CLUSTER_ID` | Reuse existing cluster (optional) |

## PySpark ETL Job Details

The PySpark job (`emr-jobs/csv_etl_job.py`) runs on EMR and processes CSV files.

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--input-path` | (required) | S3 path to input CSV |
| `--output-path` | (required) | S3 path for output |
| `--job-id` | (required) | Unique job identifier |
| `--environment` | `dev` | Environment name |
| `--delimiter` | `,` | CSV delimiter |
| `--header` | `true` | CSV has header row |
| `--infer-schema` | `true` | Infer column types |
| `--partition-by` | (none) | Column(s) to partition output |
| `--num-partitions` | `1` | Output partitions |
| `--transform` | `clean` | Transform type |
| `--aggregate-column` | (none) | Column to aggregate |
| `--aggregate-func` | `sum` | Aggregation function |
| `--drop-columns` | (none) | Columns to drop |
| `--rename-columns` | `{}` | Column rename map (JSON) |
| `--date-column` | (none) | Column to parse as date |

### Transform Types

- **`clean`** (default): Drops corrupt records, null rows, duplicates; renames/drops columns; parses dates
- **`aggregate`**: Groups by non-numeric columns and applies aggregation (sum/avg/count/min/max)
- **`passthrough`**: No transformation, writes data as-is

### Output

- **Parquet format** with Snappy compression
- **`_MANIFEST.json`** with job metrics (record counts, duration, status)
- **Partitioned** by specified columns (optional)

## Monitoring & Operations

### View Lambda logs

```bash
make logs ENVIRONMENT=dev
# Or
aws logs tail /aws/lambda/dev-emr-trigger --follow
```

### Check EMR steps

```bash
aws emr list-steps --cluster-id j-XXXXXXXX
```

### View stack outputs

```bash
make outputs ENVIRONMENT=dev
```

### Test with a sample CSV

```bash
make test-s3-event ENVIRONMENT=dev DATA_BUCKET_NAME=my-etl-data AWS_ACCOUNT_ID=123456789012
```

### Destroy the stack

```bash
make destroy ENVIRONMENT=dev
```

## Security

- **S3 buckets**: Blocked public access, versioning enabled, lifecycle policies
- **SQS queue**: Policy restricts SendMessage to the S3 bucket only
- **Lambda IAM role**: Least-privilege permissions (SQS, EMR, S3, EC2 describe)
- **GitHub OIDC**: Token-based authentication with scoped repository conditions
- **EMR**: Runs in VPC with security groups (optional)

## Cost Considerations

- **EMR transient clusters**: Only pay for compute while jobs are running
- **Auto-termination**: Clusters idle-terminate after 10 minutes
- **S3 lifecycle**: Old versions expire after 90 days
- **Lambda**: Pay per invocation (typically < $1/month for moderate usage)

## Troubleshooting

### Lambda not triggering

1. Check S3 event notification is configured on the data bucket
2. Verify SQS queue policy allows S3 to send messages
3. Check Lambda CloudWatch logs for errors

### EMR job failing

1. Check EMR step logs in `s3://{log-bucket}/emr-logs/`
2. Verify the PySpark script exists in the artifacts bucket
3. Ensure IAM roles have sufficient permissions

### SQS messages going to DLQ

1. Check DLQ for failed messages
2. Review Lambda error logs
3. Verify the SQS message format matches expected S3 event notification

## Exactly-Once Processing Guarantee

The pipeline implements a **layered defense** strategy to ensure each CSV file is processed exactly once, even in the face of failures, retries, and duplicate events. No DynamoDB needed — the distributed lock uses S3's native conditional put.

### The Problem

Without exactly-once guarantees, the following scenarios cause duplicate processing:

| Scenario | What Happens | Risk |
|----------|-------------|------|
| Lambda crashes after EMR step submit but before SQS delete | SQS message reappears → new EMR cluster + new step | **Duplicate** |
| EMR step succeeds but Lambda times out | Same as above | **Duplicate** |
| Same CSV uploaded twice | Two S3 events → two SQS messages | **Duplicate** |
| S3 event delivered more than once (S3 at-least-once) | Multiple SQS messages for same file | **Duplicate** |

### Solution: Three Layers of Defense

```
Layer 1: S3 Conditional Put (Lambda)
├── Uses IfNoneMatch='*' for atomic check-and-set
├── No DynamoDB needed — S3 is already in the pipeline
├── Lock objects stored at s3://processed-bucket/_locks/{hash}.lock
├── Prevents race conditions between concurrent invocations
└── S3 lifecycle policies auto-expire lock objects after 7 days

Layer 2: S3 Marker File (PySpark Job)
├── Checks for _dedup_markers/{file_hash}/_SUCCESS
├── Second line of defense if Lambda dedup is bypassed
├── Written after successful processing
└── Survives EMR cluster termination

Layer 3: SQS Visibility Timeout + DLQ
├── Failed messages are retried (up to 5 times)
├── Messages that exhaust retries go to DLQ
└── Manual DLQ reprocessing is safe (S3 lock prevents duplicates)
```

### How It Works

#### Lambda Layer (S3 Conditional Put)

In [`lambda/emr_trigger.py`](aws-etl-pipeline/lambda/emr_trigger.py), the `_try_acquire_lock()` function uses **S3's IfNoneMatch='*' header** as a distributed lock:

```python
s3_client.put_object(
    Bucket=lock_bucket,
    Key=f"_locks/{file_hash}.lock",
    Body=lock_payload,
    IfNoneMatch='*',  # Atomic: only succeed if object does NOT exist
)
```

- **First invocation**: `put_object` succeeds → lock acquired → process file
- **Second invocation**: `PreconditionFailed` exception → file already being processed → skip
- **Lambda crash after EMR submit**: S3 lock object exists → next retry skips the file
- **S3 unavailable**: Falls through to at-least-once (PySpark layer still protects)
- **EMR failure**: Lock is released (`DeleteObject`) so the file can be retried

Why `IfNoneMatch='*'` works as a distributed lock:
- It's an **atomic check-and-set** at the S3 API level — same semantics as DynamoDB's `ConditionExpression='attribute_not_exists(file_key)'`
- S3 is **strongly consistent** for PUT operations (since December 2020)
- No additional AWS service to manage, no IAM roles for DynamoDB
- Lock objects are automatically cleaned up by S3 lifecycle rules

#### PySpark Layer (S3 Marker)

In [`emr-jobs/csv_etl_job.py`](aws-etl-pipeline/emr-jobs/csv_etl_job.py), the `_is_already_processed()` function checks for an S3 marker file:

```python
marker_path = f"{output_path}/_dedup_markers/{file_dedup_key}/_SUCCESS"
if fs.exists(marker_path):
    logger.warning("File already processed. Exiting early.")
    return True
```

- Written at `s3://processed-bucket/output/{job-id}/_dedup_markers/{hash}/_SUCCESS`
- Checked at the start of every PySpark job invocation
- Survives EMR cluster termination and S3 lifecycle policies

### Failure Mode Analysis

| Failure Scenario | S3 Lock State | SQS Behavior | Result |
|-----------------|---------------|-------------|--------|
| Lambda crashes before S3 lock write | No lock object | Message reappears (visibility timeout) | **Retried** ✅ |
| Lambda crashes after S3 lock, before EMR submit | Lock exists | Message reappears → S3 check → **skipped** | **Exactly once** ✅ |
| Lambda crashes after EMR submit, before SQS delete | Lock exists | Message reappears → S3 check → **skipped** | **Exactly once** ✅ |
| EMR job fails | Lock released by Lambda | Lambda raises exception → releases lock → no SQS delete → retry | **Retried** ✅ |
| EMR job succeeds but Lambda times out | Lock exists | Message reappears → S3 check → **skipped** | **Exactly once** ✅ |
| Same file uploaded twice | Lock from first upload exists | Second SQS message → S3 check → **skipped** | **Exactly once** ✅ |
| S3 duplicate event (same file, same key) | Lock from first event exists | Second SQS message → S3 check → **skipped** | **Exactly once** ✅ |
| S3 service outage | N/A (fallback) | Message processed without dedup check | **At least once** ⚠️ |

### Why S3 Conditional Put Instead of DynamoDB

| Aspect | DynamoDB | S3 Conditional Put |
|--------|----------|-------------------|
| **Additional service** | Yes — must create and manage DynamoDB table | **No** — S3 is already in the pipeline |
| **Atomic check-and-set** | `ConditionExpression='attribute_not_exists(file_key)'` | `IfNoneMatch='*'` |
| **Consistency** | Eventually consistent (unless using ConsistentRead) | **Strongly consistent** for PUT operations |
| **Cost per check** | ~$0.00000125 (1 WCU) | **~$0.0000004** (1 PUT request) |
| **Cost per 1000 files** | ~$0.00125 | **~$0.0004** |
| **IAM permissions** | `dynamodb:PutItem`, `GetItem`, etc. | `s3:PutObject`, `s3:DeleteObject` |
| **Auto-cleanup** | TTL (requires DynamoDB TTL feature) | S3 lifecycle expiration rules |
| **Lock visibility** | Must query DynamoDB API | Visible as S3 objects — can `aws s3 ls` |
| **Status tracking** | Built-in (PROCESSING/COMPLETED/FAILED) | Via lock object metadata or separate status objects |

### Monitoring Dedup Activity

```bash
# List all active locks (files currently being processed or completed)
aws s3 ls s3://my-etl-processed-{account}-dev/_locks/

# Check if a specific file has a lock
aws s3api head-object \
    --bucket my-etl-processed-{account}-dev \
    --key _locks/{file_hash}.lock

# View lock metadata (original path, timestamp)
aws s3api get-object-tagging \
    --bucket my-etl-processed-{account}-dev \
    --key _locks/{file_hash}.lock

# View dedup markers in S3
aws s3 ls s3://my-etl-processed-{account}-dev/output/_dedup_markers/

# Manually release a stuck lock (if a Lambda crashed without cleanup)
aws s3 rm s3://my-etl-processed-{account}-dev/_locks/{file_hash}.lock
```

### S3 Lock Object Schema

Lock objects are stored at `s3://{processed-bucket}/_locks/{sha256_hash}.lock` with the following JSON body:

```json
{
  "original_path": "s3://data-bucket/incoming/report.csv",
  "s3_bucket": "data-bucket",
  "s3_key": "incoming/report.csv",
  "locked_at": "2026-07-08T22:00:00+00:00",
  "environment": "dev",
  "lock_version": "1"
}
```

S3 lifecycle rules on the processed bucket automatically expire lock objects after 7 days.

### Cost of Deduplication

| Resource | Cost per File | Notes |
|----------|--------------|-------|
| S3 PUT (lock acquire) | 1 PUT (~$0.0000004) | IfNoneMatch='*' conditional put |
| S3 DELETE (lock release on failure) | 1 DELETE (~$0.0000002) | Only on EMR failure |
| S3 PUT (PySpark marker) | 1 PUT (~$0.0000004) | `_dedup_markers/{hash}/_SUCCESS` |
| S3 GET (PySpark check) | 1 GET (~$0.0000004) | Idempotency check |
| **Total** | **~$0.0000012 per file** | **~6x cheaper than DynamoDB** |

## License

MIT
