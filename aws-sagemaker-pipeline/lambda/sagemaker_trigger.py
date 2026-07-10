"""
AWS Lambda function: SQS Consumer → SageMaker Pipeline Trigger

This Lambda is triggered by SQS messages when new training data is uploaded to S3.
It starts a SageMaker Pipeline execution with the new data location as input.

The pipeline handles: preprocessing → training → evaluation → model registration.

Environment Variables:
  ENVIRONMENT              : dev/staging/prod
  PIPELINE_NAME            : Name of the SageMaker pipeline to execute
  PIPELINE_ARTIFACTS_BUCKET: S3 bucket for pipeline artifacts
  DATA_BUCKET              : S3 bucket for training data
  SAGEMAKER_ROLE_ARN       : IAM role ARN for SageMaker execution
"""

import json
import os
import logging
import uuid
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
sagemaker_client = boto3.client('sagemaker')
sqs_client = boto3.client('sqs')

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
PIPELINE_NAME = os.environ.get('PIPELINE_NAME', '')
PIPELINE_ARTIFACTS_BUCKET = os.environ.get('PIPELINE_ARTIFACTS_BUCKET', '')
DATA_BUCKET = os.environ.get('DATA_BUCKET', '')
SAGEMAKER_ROLE_ARN = os.environ.get('SAGEMAKER_ROLE_ARN', '')


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
# SageMaker Pipeline Execution
# ---------------------------------------------------------------------------

def _start_pipeline_execution(s3_key: str, s3_bucket: str) -> str:
    """
    Start a SageMaker Pipeline execution.

    The pipeline receives the S3 path of the new data as a parameter.
    SageMaker Pipelines handles the workflow: preprocessing → training →
    evaluation → conditional model registration.

    Args:
        s3_key: S3 key of the new data file.
        s3_bucket: S3 bucket of the new data file.

    Returns:
        Pipeline execution ARN string.
    """
    execution_id = uuid.uuid4().hex[:12]
    input_data_url = f"s3://{s3_bucket}/{s3_key}"
    input_prefix = f"s3://{s3_bucket}/{os.path.dirname(s3_key)}"

    # Pipeline parameters that override defaults
    pipeline_parameters = {
        'InputDataUrl': input_prefix,  # Use the directory containing the new file
        'ProcessingInstanceType': 'ml.m5.xlarge',
        'TrainingInstanceType': 'ml.m5.xlarge',
        'TrainingInstanceCount': '1',
        'AccuracyThreshold': '0.75',
    }

    logger.info(
        "Starting SageMaker pipeline '%s' execution %s: input=%s",
        PIPELINE_NAME, execution_id, input_data_url,
    )

    response = sagemaker_client.start_pipeline_execution(
        PipelineName=PIPELINE_NAME,
        PipelineExecutionDisplayName=f"{PIPELINE_NAME}-{execution_id}",
        PipelineParameters=[
            {'Name': name, 'Value': value}
            for name, value in pipeline_parameters.items()
        ],
        PipelineExecutionDescription=f"Triggered by new data: {input_data_url}",
    )

    execution_arn = response['PipelineExecutionArn']
    logger.info(
        "Started pipeline execution: %s (ARN: %s)",
        execution_id, execution_arn,
    )
    return execution_arn


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
    Main Lambda handler.

    Triggered by SQS when new training data lands in S3.
    Starts a SageMaker Pipeline execution for automated retraining.
    """
    logger.info("Received event: %d records", len(event.get('Records', [])))

    processed_count = 0
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

            # Start pipeline execution for the first new file
            # (batch multiple files into one pipeline run)
            s3_record = s3_records[0]
            execution_arn = _start_pipeline_execution(
                s3_key=s3_record['key'],
                s3_bucket=s3_record['bucket'],
            )
            processed_count += 1

            # Delete the SQS message after successful processing
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
