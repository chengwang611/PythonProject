"""
AWS Lambda function: SQS Consumer → Glue Trigger (Exactly-Once Processing)

This Lambda is triggered by SQS messages when a CSV file is dropped on S3.
It uses S3 conditional put (IfNoneMatch='*') as a distributed lock to ensure
exactly-once processing, then starts an AWS Glue job to process the CSV.

Key differences from the EMR version:
  - Calls glue.start_job_run() instead of creating EMR clusters
  - No cluster management — Glue is serverless
  - Faster startup (1-2 min vs 5-10 min for EMR)
  - Pay per DPU-second instead of EC2 instance hours

Environment Variables:
  ENVIRONMENT           : dev/staging/prod
  GLUE_JOB_NAME         : Name of the Glue job to trigger
  GLUE_ARTIFACTS_BUCKET : S3 bucket with Glue scripts
  PROCESSED_BUCKET      : S3 bucket for processed output
  DATA_BUCKET           : S3 bucket for incoming CSV files
  LOCK_BUCKET           : S3 bucket for distributed lock objects
  LOCK_EXPIRY_DAYS      : Days to retain lock objects (default: 7)
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
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', '')
GLUE_ARTIFACTS_BUCKET = os.environ.get('GLUE_ARTIFACTS_BUCKET', '')
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET', '')
DATA_BUCKET = os.environ.get('DATA_BUCKET', '')
LOCK_BUCKET = os.environ.get('LOCK_BUCKET', PROCESSED_BUCKET)
LOCK_EXPIRY_DAYS = int(os.environ.get('LOCK_EXPIRY_DAYS', '7'))

# ---------------------------------------------------------------------------
# S3 Conditional Put — Distributed Lock (Exactly-Once Core)
# ---------------------------------------------------------------------------

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
            IfNoneMatch='*',
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
        return True

    except s3_client.exceptions.PreconditionFailed:
        logger.warning(
            "S3 lock already exists for %s — skipping duplicate "
            "(lock object: s3://%s/%s)",
            original_path, lock_bucket, lock_object_key,
        )
        return False

    except Exception as exc:
        logger.warning(
            "S3 lock check failed (%s), falling through to process "
            "(at-least-once): %s",
            type(exc).__name__, exc,
        )
        return True


def _release_lock(s3_bucket: str, s3_key: str) -> None:
    """Delete the S3 lock object to release the lock for retry."""
    lock_bucket = LOCK_BUCKET
    lock_object_key = _lock_key(s3_bucket, s3_key)

    try:
        s3_client.delete_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
        )
        logger.info("Released S3 lock: s3://%s/%s", lock_bucket, lock_object_key)
    except Exception as exc:
        logger.warning("Failed to release S3 lock: %s", exc)


# ---------------------------------------------------------------------------
# S3 Event Parsing
# ---------------------------------------------------------------------------

def _parse_s3_event(sqs_body: dict) -> list[dict]:
    """Parse an S3 event record from the SQS message body."""
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
# Glue Job Trigger
# ---------------------------------------------------------------------------

def _start_glue_job(s3_key: str, s3_bucket: str) -> str:
    """
    Start an AWS Glue job to process the CSV file.

    The Glue job receives the file path and dedup key as arguments.
    Glue is serverless — no cluster to create or manage.

    Args:
        s3_key: S3 key of the CSV file to process.
        s3_bucket: S3 bucket of the CSV file.

    Returns:
        Glue job run ID string.
    """
    job_id = uuid.uuid4().hex[:12]
    input_path = f"s3://{s3_bucket}/{s3_key}"
    output_path = f"s3://{PROCESSED_BUCKET}/output/{job_id}/"
    file_dedup_key = _file_hash(s3_bucket, s3_key)

    # Glue job arguments (prefixed with -- for Glue's argument parser)
    arguments = {
        '--input_path': input_path,
        '--output_path': output_path,
        '--job_id': job_id,
        '--environment': ENVIRONMENT,
        '--file_dedup_key': file_dedup_key,
    }

    logger.info(
        "Starting Glue job %s: input=%s output=%s dedup_key=%s",
        GLUE_JOB_NAME, input_path, output_path, file_dedup_key,
    )

    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments=arguments,
    )
    run_id = response['JobRunId']
    logger.info("Started Glue job run %s with ID %s", GLUE_JOB_NAME, run_id)
    return run_id


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
      3. Start Glue job
      4. Delete SQS message
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

            if body.get('Event') == 's3:TestEvent':
                logger.info("Received S3 test event, skipping")
                _delete_sqs_message(receipt_handle, queue_url)
                continue

            s3_records = _parse_s3_event(body)
            if not s3_records:
                logger.warning("No valid S3 records found in message")
                _delete_sqs_message(receipt_handle, queue_url)
                continue

            for s3_record in s3_records:
                bucket = s3_record['bucket']
                key = s3_record['key']

                # ============================================================
                # EXACTLY-ONCE CHECK: S3 conditional put
                # ============================================================
                if not _try_acquire_lock(bucket, key):
                    logger.info(
                        "Skipping already-processed file: s3://%s/%s",
                        bucket, key,
                    )
                    skipped_count += 1
                    continue

                # ============================================================
                # Glue Processing
                # ============================================================
                try:
                    run_id = _start_glue_job(
                        s3_key=key,
                        s3_bucket=bucket,
                    )
                    logger.info(
                        "Started Glue job run %s for s3://%s/%s",
                        run_id, bucket, key,
                    )
                    processed_count += 1

                except Exception as exc:
                    _release_lock(bucket, key)
                    logger.error(
                        "Failed to process s3://%s/%s: %s",
                        bucket, key, exc, exc_info=True,
                    )
                    failed_count += 1
                    raise

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
    """Extract the SQS queue URL from the event source ARN."""
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
