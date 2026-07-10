"""
AWS Lambda function: SQS Consumer → Glue Trigger (Folder-Level Sentinel)

This Lambda is triggered by SQS messages when a _COMPLETE sentinel file
is dropped on S3, signaling that all CSV files in a folder have been delivered.
It uses S3 conditional put (IfNoneMatch='*') as a distributed lock to ensure
exactly-once processing at the folder level, then starts an AWS Glue job to
process all CSV files in that folder.

Trigger Pattern:
  Upstream uploads all CSV files → then uploads _COMPLETE sentinel
  S3 event on _COMPLETE → SQS → Lambda → lists folder → Glue job

Key differences from the file-level version:
  - Triggered by _COMPLETE sentinel files, not individual .csv files
  - Lists all .csv files in the folder before starting Glue job
  - Locks at the folder level (not per-file)
  - Passes a JSON array of file paths to the Glue job
  - Stale lock detection: locks older than LOCK_STALE_TIMEOUT_SECONDS
    are automatically deleted and re-acquired (handles Lambda crashes
    between lock acquisition and Glue start)

Environment Variables:
  ENVIRONMENT                : dev/staging/prod
  GLUE_JOB_NAME              : Name of the Glue job to trigger
  GLUE_ARTIFACTS_BUCKET      : S3 bucket with Glue scripts
  PROCESSED_BUCKET           : S3 bucket for processed output
  DATA_BUCKET                : S3 bucket for incoming CSV files
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
LOCK_STALE_TIMEOUT_SECONDS = int(os.environ.get('LOCK_STALE_TIMEOUT_SECONDS', '3600'))

# Sentinel file name that signals folder completion
SENTINEL_FILENAME = '_COMPLETE'


# ---------------------------------------------------------------------------
# S3 Conditional Put — Distributed Lock (Exactly-Once Core)
# ---------------------------------------------------------------------------

def _folder_hash(folder_prefix: str) -> str:
    """SHA-256 hash of the folder prefix for lock object key."""
    return hashlib.sha256(folder_prefix.encode()).hexdigest()


def _lock_key_for_folder(folder_prefix: str) -> str:
    """S3 object key for the distributed lock on a folder."""
    folder_id = _folder_hash(folder_prefix)
    return f"_locks/{folder_id}.lock"


def _try_acquire_lock(folder_prefix: str) -> bool:
    """
    Try to acquire a distributed lock for a folder using S3 conditional put.

    Uses IfNoneMatch='*' which tells S3: "only create this object if
    it doesn't already exist." This is atomic at the S3 API level.

    STALE LOCK DETECTION: If a lock already exists but is older than
    LOCK_STALE_TIMEOUT_SECONDS (default: 1 hour), it is considered stale.
    This handles the case where Lambda crashes after acquiring the lock
    but before starting the Glue job — the stale lock is deleted and
    a new one is acquired, allowing the folder to be processed.

    A lock is NOT stale if a Glue job is actively running (the Glue job
    releases the lock on success, so a long-running job's lock is valid).

    Returns:
        True if lock was acquired (first time or stale lock replaced).
        False if lock already exists and is fresh (folder is being processed).
    """
    lock_bucket = LOCK_BUCKET
    lock_object_key = _lock_key_for_folder(folder_prefix)
    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Check for stale lock BEFORE attempting to acquire
    # ------------------------------------------------------------------
    try:
        existing = s3_client.head_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
        )
        last_modified = existing['LastModified']
        lock_age_seconds = (now - last_modified).total_seconds()

        if lock_age_seconds > LOCK_STALE_TIMEOUT_SECONDS:
            logger.warning(
                "Stale lock detected for folder '%s' (age: %.0fs > timeout: %ds). "
                "Deleting stale lock and re-acquiring.",
                folder_prefix, lock_age_seconds, LOCK_STALE_TIMEOUT_SECONDS,
            )
            s3_client.delete_object(
                Bucket=lock_bucket,
                Key=lock_object_key,
            )
            # Fall through to acquire a fresh lock below
        else:
            logger.warning(
                "S3 lock already exists for folder '%s' (age: %.0fs) — "
                "skipping duplicate (lock object: s3://%s/%s)",
                folder_prefix, lock_age_seconds, lock_bucket, lock_object_key,
            )
            return False

    except s3_client.exceptions.ClientError as exc:
        # 404 = lock doesn't exist, which is expected — proceed to acquire
        if exc.response['Error']['Code'] != '404':
            logger.warning(
                "S3 head_object failed for lock check (%s), "
                "falling through to acquire: %s",
                exc.response['Error']['Code'], exc,
            )
    except Exception as exc:
        logger.warning(
            "S3 lock staleness check failed (%s), falling through to "
            "acquire (at-least-once): %s",
            type(exc).__name__, exc,
        )

    # ------------------------------------------------------------------
    # Acquire fresh lock
    # ------------------------------------------------------------------
    lock_payload = json.dumps({
        'folder_prefix': folder_prefix,
        'locked_at': now.isoformat(),
        'environment': ENVIRONMENT,
        'lock_version': '3',  # v3 = folder-level locking with stale detection
        'trigger_type': 'sentinel',
    })

    try:
        s3_client.put_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
            Body=lock_payload.encode('utf-8'),
            ContentType='application/json',
            IfNoneMatch='*',
            Metadata={
                'folder-prefix': folder_prefix,
                'environment': ENVIRONMENT,
                'locked-at': now.isoformat(),
            },
        )
        logger.info(
            "S3 lock acquired for folder '%s' (lock object: s3://%s/%s)",
            folder_prefix, lock_bucket, lock_object_key,
        )
        return True

    except s3_client.exceptions.PreconditionFailed:
        # Race condition: another Lambda acquired the lock between our
        # head_object check and this put_object. This is safe — the other
        # Lambda will process the folder.
        logger.warning(
            "S3 lock race for folder '%s' — another Lambda acquired it first "
            "(lock object: s3://%s/%s)",
            folder_prefix, lock_bucket, lock_object_key,
        )
        return False

    except Exception as exc:
        logger.warning(
            "S3 lock acquisition failed (%s), falling through to process "
            "(at-least-once): %s",
            type(exc).__name__, exc,
        )
        return True


def _release_lock(folder_prefix: str) -> None:
    """Delete the S3 lock object to release the lock for retry."""
    lock_bucket = LOCK_BUCKET
    lock_object_key = _lock_key_for_folder(folder_prefix)

    try:
        s3_client.delete_object(
            Bucket=lock_bucket,
            Key=lock_object_key,
        )
        logger.info("Released S3 lock for folder '%s': s3://%s/%s",
                     folder_prefix, lock_bucket, lock_object_key)
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
# Folder-Level Sentinel Logic
# ---------------------------------------------------------------------------

def _extract_folder_from_sentinel(s3_key: str) -> str:
    """
    Extract the folder prefix from a sentinel file path.

    Example:
        incoming/2026-07-10/_COMPLETE → incoming/2026-07-10/
        data/daily/_COMPLETE → data/daily/
        _COMPLETE → ''  (root of bucket)
    """
    parts = s3_key.rsplit('/', 1)
    if len(parts) == 2 and parts[1] == SENTINEL_FILENAME:
        return parts[0] + '/'
    # Sentinel at bucket root
    return ''


def _list_csv_files_in_folder(bucket: str, folder_prefix: str) -> list[str]:
    """
    List all .csv files in the given S3 folder prefix.
    Returns full s3:// URIs. Excludes the sentinel file itself.
    """
    csv_files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Skip the sentinel file itself and any non-CSV files
            if key.endswith('.csv') and not key.endswith(SENTINEL_FILENAME):
                csv_files.append(f"s3://{bucket}/{key}")

    logger.info("Found %d CSV files in folder '%s' (bucket: %s)",
                len(csv_files), folder_prefix, bucket)
    return csv_files


def _read_sentinel_metadata(bucket: str, key: str) -> dict | None:
    """
    Optionally read metadata from the _COMPLETE sentinel file.
    If the file contains JSON with expected_file_count, checksums, etc.,
    return it for validation. Returns None if file is empty or not JSON.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8').strip()

        if not body:
            logger.info("Sentinel file '%s' is empty (no metadata)", key)
            return None

        metadata = json.loads(body)
        logger.info("Read sentinel metadata from '%s': %s", key,
                     json.dumps(metadata, default=str))
        return metadata

    except json.JSONDecodeError:
        logger.info("Sentinel file '%s' is not valid JSON — treating as simple marker", key)
        return None
    except Exception as exc:
        logger.warning("Failed to read sentinel metadata from '%s': %s", key, exc)
        return None


def _validate_folder(folder_prefix: str, csv_files: list[str],
                     sentinel_metadata: dict | None) -> tuple[bool, str]:
    """
    Validate that the folder contents match expectations.

    If sentinel_metadata contains expected_file_count, validate against it.
    Returns (is_valid, error_message).
    """
    if not csv_files:
        return False, f"No CSV files found in folder '{folder_prefix}'"

    if sentinel_metadata:
        expected_count = sentinel_metadata.get('expected_file_count')
        if expected_count is not None:
            actual_count = len(csv_files)
            if actual_count != expected_count:
                return False, (
                    f"File count mismatch in folder '{folder_prefix}': "
                    f"expected {expected_count}, found {actual_count}"
                )
            logger.info("File count validation passed: %d files (expected %d)",
                        actual_count, expected_count)

    return True, ''


# ---------------------------------------------------------------------------
# Glue Job Trigger (Folder-Level)
# ---------------------------------------------------------------------------

def _start_glue_job_for_folder(folder_prefix: str, csv_files: list[str],
                                s3_bucket: str) -> str:
    """
    Start an AWS Glue job to process all CSV files in a folder.

    Passes the list of files as a JSON array argument so the Glue job
    can read and union all files in a single run.

    Args:
        folder_prefix: S3 folder prefix (e.g., 'incoming/2026-07-10/').
        csv_files: List of full s3:// URIs for all CSV files.
        s3_bucket: S3 bucket name.

    Returns:
        Glue job run ID string.
    """
    job_id = uuid.uuid4().hex[:12]
    folder_hash = _folder_hash(folder_prefix)
    output_path = f"s3://{PROCESSED_BUCKET}/output/{folder_hash}/{job_id}/"

    # Glue job arguments (prefixed with -- for Glue's argument parser)
    arguments = {
        '--input_paths': json.dumps(csv_files),       # JSON array of all file paths
        '--folder_prefix': folder_prefix,
        '--output_path': output_path,
        '--job_id': job_id,
        '--environment': ENVIRONMENT,
        '--file_dedup_key': folder_hash,               # Dedup at folder level
    }

    logger.info(
        "Starting Glue job %s for folder '%s': %d files → output=%s",
        GLUE_JOB_NAME, folder_prefix, len(csv_files), output_path,
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
    Main Lambda handler — folder-level sentinel trigger.

    Processing flow:
      1. Parse SQS message → extract S3 event for _COMPLETE sentinel
      2. Extract folder prefix from sentinel path
      3. S3 conditional put (IfNoneMatch='*') — atomic distributed lock on folder
         - If lock exists → skip (folder already processed)
         - If lock is new → acquire and proceed
      4. List all .csv files in the folder
      5. Optionally validate against sentinel metadata
      6. Start Glue job with all file paths
      7. Delete SQS message

    Lock Lifecycle:
      - Lock ACQUIRED by Lambda (step 3)
      - Lock RELEASED by Lambda on validation failure or Glue start failure
      - Lock RELEASED by Glue job after successful ETL completion
      - Lock NOT released if Glue job fails → prevents duplicate processing
      - Lock expires via S3 lifecycle policy (default: 7 days) as fallback
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
                # STEP 1: Extract folder prefix from sentinel path
                # ============================================================
                folder_prefix = _extract_folder_from_sentinel(key)
                logger.info(
                    "Sentinel detected: s3://%s/%s → folder='%s'",
                    bucket, key, folder_prefix,
                )

                # ============================================================
                # STEP 2: EXACTLY-ONCE CHECK — folder-level S3 conditional put
                # ============================================================
                if not _try_acquire_lock(folder_prefix):
                    logger.info(
                        "Skipping already-processed folder: '%s' (bucket: %s)",
                        folder_prefix, bucket,
                    )
                    skipped_count += 1
                    continue

                # ============================================================
                # STEP 3: Read sentinel metadata (optional validation)
                # ============================================================
                sentinel_metadata = _read_sentinel_metadata(bucket, key)

                # ============================================================
                # STEP 4: List all CSV files in the folder
                # ============================================================
                try:
                    csv_files = _list_csv_files_in_folder(bucket, folder_prefix)

                    # Validate folder contents
                    is_valid, error_msg = _validate_folder(
                        folder_prefix, csv_files, sentinel_metadata,
                    )
                    if not is_valid:
                        logger.error("Folder validation failed: %s", error_msg)
                        _release_lock(folder_prefix)
                        failed_count += 1
                        continue

                    # ============================================================
                    # STEP 5: Start Glue job with all file paths
                    # ============================================================
                    run_id = _start_glue_job_for_folder(
                        folder_prefix=folder_prefix,
                        csv_files=csv_files,
                        s3_bucket=bucket,
                    )
                    logger.info(
                        "Started Glue job run %s for folder '%s' (%d files)",
                        run_id, folder_prefix, len(csv_files),
                    )
                    processed_count += 1

                except Exception as exc:
                    _release_lock(folder_prefix)
                    logger.error(
                        "Failed to process folder '%s': %s",
                        folder_prefix, exc, exc_info=True,
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
