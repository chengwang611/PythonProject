"""
AWS Lambda function: SQS Consumer → EMR Trigger (Exactly-Once Processing)

This Lambda is triggered by SQS messages when a CSV file is dropped on S3.
It uses S3 conditional put (IfNoneMatch='*') as a distributed lock to ensure
exactly-once processing — no DynamoDB needed.

Processing flow:
  1. Parse SQS message → extract S3 event
  2. S3 conditional put (atomic check-and-set using IfNoneMatch='*')
     - If lock object exists → skip (already processed or in progress)
     - If lock is new → acquire and proceed
  3. Create/get EMR cluster
  4. Submit PySpark step
  5. Delete SQS message

Why S3 conditional put instead of DynamoDB:
  - No additional AWS service to manage
  - S3 is already in the pipeline (data bucket exists)
  - IfNoneMatch='*' provides the same atomic check-and-set as DynamoDB
  - S3 lifecycle policies can auto-expire lock objects
  - Lower cost: ~$0.0000004 per check vs ~$0.00000125 for DynamoDB

Environment Variables:
  ENVIRONMENT              : dev/staging/prod
  EMR_RELEASE_LABEL        : EMR release label (e.g., emr-7.5.0)
  EMR_MASTER_INSTANCE_TYPE : EC2 instance type for master node
  EMR_CORE_INSTANCE_TYPE   : EC2 instance type for core nodes
  EMR_CORE_INSTANCE_COUNT  : Number of core instances
  EMR_SERVICE_ROLE         : IAM role for EMR service
  EMR_EC2_ROLE             : IAM role for EMR EC2 instances
  EMR_LOG_URI              : S3 URI for EMR logs
  EMR_ARTIFACTS_BUCKET     : S3 bucket with EMR scripts
  EMR_JOB_TIMEOUT          : Step timeout in seconds
  PROCESSED_BUCKET         : S3 bucket for processed output
  DATA_BUCKET              : S3 bucket for incoming CSV files
  SUBNET_ID                : Subnet ID for EMR cluster
  VPC_ID                   : VPC ID for EMR cluster
  KEY_NAME                 : EC2 key pair name (optional)
  LOCK_BUCKET              : S3 bucket for distributed lock objects (default: PROCESSED_BUCKET)
  LOCK_EXPIRY_DAYS         : Days to retain lock objects (default: 7)
"""

import json
import os
import logging
import uuid
import hashlib
from datetime import datetime, timezone

import boto3
from urllib.parse import unquote_plus

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
emr_client = boto3.client('emr')
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
EMR_RELEASE_LABEL = os.environ.get('EMR_RELEASE_LABEL', 'emr-7.5.0')
EMR_MASTER_INSTANCE_TYPE = os.environ.get('EMR_MASTER_INSTANCE_TYPE', 'm5.xlarge')
EMR_CORE_INSTANCE_TYPE = os.environ.get('EMR_CORE_INSTANCE_TYPE', 'm5.xlarge')
EMR_CORE_INSTANCE_COUNT = int(os.environ.get('EMR_CORE_INSTANCE_COUNT', '2'))
EMR_SERVICE_ROLE = os.environ.get('EMR_SERVICE_ROLE', 'EMR_DefaultRole')
EMR_EC2_ROLE = os.environ.get('EMR_EC2_ROLE', 'EMR_EC2_DefaultRole')
EMR_LOG_URI = os.environ.get('EMR_LOG_URI', '')
EMR_ARTIFACTS_BUCKET = os.environ.get('EMR_ARTIFACTS_BUCKET', '')
EMR_JOB_TIMEOUT = int(os.environ.get('EMR_JOB_TIMEOUT', '3600'))
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET', '')
DATA_BUCKET = os.environ.get('DATA_BUCKET', '')
SUBNET_ID = os.environ.get('SUBNET_ID', '')
VPC_ID = os.environ.get('VPC_ID', '')
KEY_NAME = os.environ.get('KEY_NAME', '')
LOCK_BUCKET = os.environ.get('LOCK_BUCKET', PROCESSED_BUCKET)
LOCK_EXPIRY_DAYS = int(os.environ.get('LOCK_EXPIRY_DAYS', '7'))

# ---------------------------------------------------------------------------
# EMR Cluster configuration
# ---------------------------------------------------------------------------
CLUSTER_NAME = f"{ENVIRONMENT}-csv-etl-cluster"
LONG_RUNNING_CLUSTER_ID = os.environ.get('LONG_RUNNING_CLUSTER_ID', '')

# ---------------------------------------------------------------------------
# S3 Conditional Put — Distributed Lock (Exactly-Once Core)
# ---------------------------------------------------------------------------
# How it works:
#   S3's PutObject with IfNoneMatch='*' is an atomic operation:
#   - If the object does NOT exist → put succeeds → lock acquired
#   - If the object EXISTS → PreconditionFailed → lock already held
#
# This is the same pattern as DynamoDB's conditional write, but using S3
# which is already part of the pipeline. No additional service needed.

def _file_key(s3_bucket: str, s3_key: str) -> str:
    """Canonical file identifier."""
    return f"s3://{s3_bucket}/{s3_key}"


def _file_hash(s3_bucket: str, s3_key: str) -> str:
    """SHA-256 hash of the canonical file path for lock object key."""
    return hashlib.sha256(_file_key(s3_bucket, s3_key).encode()).hexdigest()


def _lock_key(s3_bucket: str, s3_key: str) -> str:
    """S3 object key for the distributed lock."""
    file_id = _file_hash(s3_bucket, s3_key)
    return f"_locks/{file_id}.lock"


def _try_acquire_lock(s3_bucket: str, s3_key: str) -> bool:
    """
    Try to acquire a distributed lock using S3 conditional put.

    Uses IfNoneMatch='*' which tells S3: "only create this object if
    it doesn't already exist." This is atomic at the S3 API level.

    Returns:
        True if lock was acquired (first time seeing this file).
        False if lock already exists (file is being processed or was processed).
    """
    lock_bucket = LOCK_BUCKET
    lock_object_key = _lock_key(s3_bucket, s3_key)
    original_path = _file_key(s3_bucket, s3_key)
    now = datetime.now(timezone.utc)

    lock_payload = json.dumps({
        'original_path': original_path,
        's3_bucket': s3_bucket,
        's3_key': s3_key,
        'locked_at': now.isoformat(),
        'environment': ENVIRONMENT,
        'lock_version': '1',
    })

    try:
        s3_client.put_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
            Body=lock_payload.encode('utf-8'),
            ContentType='application/json',
            IfNoneMatch='*',  # Atomic: only succeed if object does NOT exist
            Metadata={
                'original-path': original_path,
                'environment': ENVIRONMENT,
                'locked-at': now.isoformat(),
            },
        )
        logger.info(
            "S3 lock acquired for %s (lock object: s3://%s/%s)",
            original_path, lock_bucket, lock_object_key,
        )
        return True  # Lock acquired — first time processing this file

    except s3_client.exceptions.PreconditionFailed:
        # IfNoneMatch='*' failed because the object already exists
        logger.warning(
            "S3 lock already exists for %s — skipping duplicate "
            "(lock object: s3://%s/%s)",
            original_path, lock_bucket, lock_object_key,
        )
        return False  # Lock already held — duplicate

    except Exception as exc:
        # If S3 is unavailable, fall through to process
        # This is at-least-once fallback behavior
        logger.warning(
            "S3 lock check failed (%s), falling through to process "
            "(at-least-once): %s",
            type(exc).__name__, exc,
        )
        return True  # Allow processing (at-least-once fallback)


def _release_lock(s3_bucket: str, s3_key: str) -> None:
    """
    Delete the S3 lock object to release the lock.

    Used when we need to allow retry (e.g., transient error before EMR submission).
    """
    lock_bucket = LOCK_BUCKET
    lock_object_key = _lock_key(s3_bucket, s3_key)

    try:
        s3_client.delete_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
        )
        logger.info(
            "Released S3 lock: s3://%s/%s",
            lock_bucket, lock_object_key,
        )
    except Exception as exc:
        logger.warning("Failed to release S3 lock: %s", exc)


# ---------------------------------------------------------------------------
# S3 Event Parsing
# ---------------------------------------------------------------------------

def _parse_s3_event(sqs_body: dict) -> list[dict]:
    """
    Parse an S3 event record from the SQS message body.

    Returns a list of dicts with 'bucket', 'key', 'size', and 'etag'.
    """
    records = []
    try:
        s3_events = sqs_body.get('Records', [])
    except AttributeError:
        logger.error("Invalid SQS body format: %s", sqs_body)
        return records

    for event in s3_events:
        try:
            bucket = event['s3']['bucket']['name']
            key = unquote_plus(event['s3']['object']['key'])
            size = event['s3']['object'].get('size', 0)
            etag = event['s3']['object'].get('eTag', '')
            records.append({
                'bucket': bucket,
                'key': key,
                'size': size,
                'etag': etag,
            })
            logger.info("Parsed S3 event: bucket=%s key=%s size=%d etag=%s",
                        bucket, key, size, etag)
        except KeyError as exc:
            logger.warning("Missing key in S3 event record: %s", exc)
            continue

    return records


# ---------------------------------------------------------------------------
# EMR Cluster Management
# ---------------------------------------------------------------------------

def _get_or_create_cluster() -> str:
    """Return existing long-running cluster or create a new transient cluster."""
    if LONG_RUNNING_CLUSTER_ID:
        logger.info("Using long-running cluster: %s", LONG_RUNNING_CLUSTER_ID)
        return LONG_RUNNING_CLUSTER_ID

    return _create_transient_cluster()


def _create_transient_cluster() -> str:
    """Create a transient EMR cluster that auto-terminates after steps."""
    instances_config = {
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': EMR_MASTER_INSTANCE_TYPE,
                'InstanceCount': 1,
            },
            {
                'Name': 'Core',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': EMR_CORE_INSTANCE_TYPE,
                'InstanceCount': EMR_CORE_INSTANCE_COUNT,
            },
        ],
    }

    if SUBNET_ID:
        instances_config['Ec2SubnetId'] = SUBNET_ID
    if KEY_NAME:
        instances_config['Ec2KeyName'] = KEY_NAME

    kwargs = {
        'Name': f"{CLUSTER_NAME}-{uuid.uuid4().hex[:8]}",
        'ReleaseLabel': EMR_RELEASE_LABEL,
        'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
        'Instances': instances_config,
        'ServiceRole': EMR_SERVICE_ROLE,
        'JobFlowRole': EMR_EC2_ROLE,
        'LogUri': EMR_LOG_URI,
        'VisibleToAllUsers': True,
        'AutoTerminationPolicy': {'IdleTimeout': 600},
        'Tags': [
            {'Key': 'Environment', 'Value': ENVIRONMENT},
            {'Key': 'Name', 'Value': CLUSTER_NAME},
            {'Key': 'ManagedBy', 'Value': 'Lambda'},
        ],
    }

    logger.info("Creating transient EMR cluster: %s", kwargs['Name'])
    response = emr_client.run_job_flow(**kwargs)
    cluster_id = response['JobFlowId']
    logger.info("Created EMR cluster: %s", cluster_id)
    return cluster_id


# ---------------------------------------------------------------------------
# EMR Step Submission
# ---------------------------------------------------------------------------

def _submit_pyspark_step(cluster_id: str, s3_key: str, s3_bucket: str) -> str:
    """
    Submit a PySpark step to the EMR cluster.

    Passes the file dedup hash to the PySpark job so it can perform
    its own idempotency check as a second line of defense.
    """
    job_id = uuid.uuid4().hex[:12]
    input_path = f"s3://{s3_bucket}/{s3_key}"
    output_path = f"s3://{PROCESSED_BUCKET}/output/{job_id}/"
    script_path = f"s3://{EMR_ARTIFACTS_BUCKET}/emr-jobs/csv_etl_job.py"
    file_dedup_key = _file_hash(s3_bucket, s3_key)

    script_args = [
        script_path,
        "--input-path", input_path,
        "--output-path", output_path,
        "--job-id", job_id,
        "--environment", ENVIRONMENT,
        "--file-dedup-key", file_dedup_key,
    ]

    step = {
        'Name': f"CSV-ETL-{job_id}",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit'] + script_args,
        },
    }

    logger.info(
        "Submitting step to cluster %s: input=%s output=%s dedup_key=%s",
        cluster_id, input_path, output_path, file_dedup_key,
    )

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step],
    )
    step_id = response['StepIds'][0]
    logger.info("Submitted step %s with ID %s", step['Name'], step_id)
    return step_id


# ---------------------------------------------------------------------------
# SQS Message Management
# ---------------------------------------------------------------------------

def _delete_sqs_message(receipt_handle: str, queue_url: str) -> None:
    """Delete a processed message from the SQS queue."""
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.info("Deleted SQS message")
    except Exception as exc:
        logger.warning("Failed to delete SQS message: %s", exc)


# ---------------------------------------------------------------------------
# Main Handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda handler with exactly-once processing guarantees.

    Processing flow:
      1. Parse SQS message → extract S3 event
      2. S3 conditional put (IfNoneMatch='*') — atomic distributed lock
         - If lock exists → skip (already processed)
         - If lock is new → acquire and proceed
      3. Create/get EMR cluster
      4. Submit PySpark step
      5. Delete SQS message

    Failure handling:
      - If EMR submission fails → release S3 lock → SQS retries
      - If Lambda crashes after EMR submit but before SQS delete →
        S3 lock prevents reprocessing (exactly-once)
      - If S3 is unavailable → falls through to process (at-least-once fallback)
    """
    logger.info("Received event: %d records", len(event.get('Records', [])))

    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for record in event.get('Records', []):
        receipt_handle = record.get('receiptHandle', '')
        queue_url = _extract_queue_url(record)

        try:
            body = json.loads(record['body'])

            # Handle S3 test events
            if body.get('Event') == 's3:TestEvent':
                logger.info("Received S3 test event, skipping")
                _delete_sqs_message(receipt_handle, queue_url)
                continue

            # Parse S3 event records
            s3_records = _parse_s3_event(body)
            if not s3_records:
                logger.warning("No valid S3 records found in message")
                _delete_sqs_message(receipt_handle, queue_url)
                continue

            # Process each CSV file in the S3 event
            for s3_record in s3_records:
                bucket = s3_record['bucket']
                key = s3_record['key']

                # ============================================================
                # EXACTLY-ONCE CHECK: S3 conditional put (IfNoneMatch='*')
                # ============================================================
                if not _try_acquire_lock(bucket, key):
                    logger.info(
                        "Skipping already-processed file: s3://%s/%s",
                        bucket, key,
                    )
                    skipped_count += 1
                    continue

                # ============================================================
                # EMR Processing
                # ============================================================
                try:
                    cluster_id = _get_or_create_cluster()
                    step_id = _submit_pyspark_step(
                        cluster_id=cluster_id,
                        s3_key=key,
                        s3_bucket=bucket,
                    )

                    logger.info(
                        "Submitted EMR step %s for s3://%s/%s",
                        step_id, bucket, key,
                    )
                    processed_count += 1

                except Exception as exc:
                    # Release the S3 lock so the file can be retried
                    _release_lock(bucket, key)
                    logger.error(
                        "Failed to process s3://%s/%s: %s",
                        bucket, key, exc, exc_info=True,
                    )
                    failed_count += 1
                    raise  # Prevent SQS deletion → message retries

            # Delete SQS message only after ALL files in the batch succeeded
            _delete_sqs_message(receipt_handle, queue_url)

        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in SQS message: %s", exc)
            failed_count += 1
        except Exception as exc:
            logger.error(
                "Error processing SQS message (will retry): %s",
                exc, exc_info=True,
            )
            failed_count += 1

    result = {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'skipped': skipped_count,
            'failed': failed_count,
        }),
    }
    logger.info("Result: %s", result)
    return result


def _extract_queue_url(record: dict) -> str:
    """
    Extract the SQS queue URL from the event source ARN.

    ARN format: arn:aws:sqs:region:account-id:queue-name
    URL format: https://sqs.region.amazonaws.com/account-id/queue-name
    """
    try:
        event_source_arn = record.get('eventSourceARN', '')
        parts = event_source_arn.split(':')
        if len(parts) >= 6:
            region = parts[3]
            account_id = parts[4]
            queue_name = parts[5]
            return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    except Exception:
        pass
    return ''
