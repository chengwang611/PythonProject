#!/usr/bin/env python3
"""
AWS Glue PySpark ETL Job for CSV Processing (Idempotent)

This script runs as an AWS Glue job, triggered by the Lambda function.
It reads CSV files from S3, performs transformations, and writes the
processed output back to S3 in Parquet format (with optional partitioning).

IDEMPOTENCY: This job checks for an existing output marker before processing.
If the output already exists (from a previous run), the job exits early.
This provides a second line of defense for exactly-once processing.

Arguments (passed by Lambda via Glue's -- prefix convention):
    --input_path       S3 path to input CSV file (required)
    --output_path      S3 path for output (required)
    --job_id           Unique job identifier (required)
    --environment      Environment name (default: dev)
    --file_dedup_key   SHA-256 hash of the input file path for dedup (optional)

Optional arguments:
    --delimiter          CSV delimiter (default: comma)
    --header             Whether CSV has header (default: true)
    --infer_schema       Infer schema from CSV (default: true)
    --partition_by       Column(s) to partition output by
    --num_partitions     Number of output partitions (default: 1)
    --transform          Transformation type: clean, aggregate, passthrough (default: clean)
    --aggregate_column   Column to aggregate on
    --aggregate_func     Aggregation function: sum, avg, count, min, max (default: sum)
    --drop_columns       Comma-separated columns to drop
    --rename_columns     JSON map of old->new column names
    --date_column        Column to parse as date
"""

import os
import sys
import json
import logging
import hashlib
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing (Glue passes arguments with -- prefix)
# ---------------------------------------------------------------------------
def parse_args() -> dict:
    """
    Parse Glue job arguments.

    Glue passes arguments as: --argument_name value
    We strip the leading -- and convert underscores to hyphens.
    """
    args = {}
    raw_args = sys.argv[1:] if len(sys.argv) > 1 else []

    i = 0
    while i < len(raw_args):
        if raw_args[i].startswith('--'):
            key = raw_args[i][2:].replace('-', '_')
            if i + 1 < len(raw_args) and not raw_args[i + 1].startswith('--'):
                args[key] = raw_args[i + 1]
                i += 2
            else:
                args[key] = True
                i += 1
        else:
            i += 1

    return args


def get_arg(args: dict, key: str, default=None):
    """Get an argument value with a default."""
    return args.get(key, default)


# ---------------------------------------------------------------------------
# Spark Session (Glue-specific)
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure a Spark session for Glue."""
    try:
        from awsglue.context import GlueContext
        glue_context = GlueContext(SparkSession.builder.appName(app_name).getOrCreate())
        spark = glue_context.spark_session
        logger.info("Using GlueContext for Spark session")
    except ImportError:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
            .config('spark.sql.adaptive.enabled', 'true') \
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
            .getOrCreate()
        logger.info("Using standard SparkSession (no GlueContext available)")

    spark.sparkContext.setLogLevel('WARN')
    return spark


# ---------------------------------------------------------------------------
# Idempotency Check
# ---------------------------------------------------------------------------

def _is_already_processed(spark: SparkSession, output_path: str, file_dedup_key: str) -> bool:
    """
    Idempotency check: verify if this file has already been processed.

    Checks for a _SUCCESS marker file at a deterministic location based on
    the file's dedup key. This is a second line of defense after the
    S3 conditional put check in the Lambda function.
    """
    if not file_dedup_key:
        return False

    marker_path = f"{output_path.rstrip('/')}/_dedup_markers/{file_dedup_key}/_SUCCESS"

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(marker_path)
        exists = fs.exists(path)

        if exists:
            logger.warning(
                "IDEMPOTENCY: File already processed (marker exists at %s). Exiting early.",
                marker_path,
            )
        else:
            logger.info("Idempotency check passed: no existing marker at %s", marker_path)

        return exists

    except Exception as exc:
        logger.warning(
            "Idempotency check failed (%s), proceeding with processing: %s",
            type(exc).__name__, exc,
        )
        return False


def _write_processed_marker(spark: SparkSession, output_path: str, file_dedup_key: str) -> None:
    """Write a _SUCCESS marker file to indicate this file was processed."""
    if not file_dedup_key:
        return

    marker_path = f"{output_path.rstrip('/')}/_dedup_markers/{file_dedup_key}/_SUCCESS"

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(marker_path)

        if not fs.exists(path.getParent()):
            fs.mkdirs(path.getParent())

        fs.create(path).close()
        logger.info("Wrote idempotency marker: %s", marker_path)

    except Exception as exc:
        logger.warning("Failed to write idempotency marker: %s", exc)


# ---------------------------------------------------------------------------
# Data reading
# ---------------------------------------------------------------------------

def read_csv(spark: SparkSession, input_path: str, delimiter: str,
             header: bool, infer_schema: bool):
    """Read a CSV file from S3 into a Spark DataFrame."""
    logger.info("Reading CSV from: %s", input_path)

    reader = spark.read \
        .format('csv') \
        .option('delimiter', delimiter) \
        .option('header', str(header).lower()) \
        .option('inferSchema', str(infer_schema).lower()) \
        .option('mode', 'PERMISSIVE') \
        .option('columnNameOfCorruptRecord', '_corrupt_record')

    df = reader.load(input_path)

    record_count = df.count()
    column_count = len(df.columns)
    logger.info("Loaded %d records with %d columns from %s", record_count, column_count, input_path)

    return df


def read_csv_files(spark: SparkSession, input_paths_json: str,
                   delimiter: str, header: bool, infer_schema: bool):
    """
    Read multiple CSV files from a JSON array of S3 paths.
    Unions all files into a single DataFrame using unionByName
    (handles column order differences between files).

    Used in folder-level sentinel mode where the Lambda passes
    all CSV file paths in the folder as a JSON array.
    """
    paths = json.loads(input_paths_json)

    if not paths:
        raise ValueError("Empty file list provided to read_csv_files")

    logger.info("Reading %d CSV files in folder mode", len(paths))

    dfs = []
    for i, path in enumerate(paths):
        logger.info("  [%d/%d] Reading: %s", i + 1, len(paths), path)
        df = spark.read \
            .format('csv') \
            .option('delimiter', delimiter) \
            .option('header', str(header).lower()) \
            .option('inferSchema', str(infer_schema).lower()) \
            .option('mode', 'PERMISSIVE') \
            .option('columnNameOfCorruptRecord', '_corrupt_record') \
            .load(path)
        dfs.append(df)

    # Union all DataFrames (unionByName handles column order differences)
    from functools import reduce
    from pyspark.sql import DataFrame

    if len(dfs) == 1:
        combined = dfs[0]
    else:
        combined = reduce(DataFrame.unionByName, dfs)

    record_count = combined.count()
    column_count = len(combined.columns)
    logger.info("Combined %d files: %d records, %d columns",
                len(paths), record_count, column_count)

    return combined


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def clean_transform(df, args: dict):
    """Apply cleaning transformations: drop corrupt, drop columns, rename, dedup."""
    logger.info("Applying clean transformation")

    if '_corrupt_record' in df.columns:
        corrupt_count = df.filter(F.col('_corrupt_record').isNotNull()).count()
        if corrupt_count > 0:
            logger.warning("Dropping %d corrupt records", corrupt_count)
        df = df.filter(F.col('_corrupt_record').isNull()).drop('_corrupt_record')

    drop_columns = get_arg(args, 'drop_columns', '')
    if drop_columns:
        drop_cols = [c.strip() for c in drop_columns.split(',')]
        existing_drop = [c for c in drop_cols if c in df.columns]
        if existing_drop:
            logger.info("Dropping columns: %s", existing_drop)
            df = df.drop(*existing_drop)

    rename_columns = get_arg(args, 'rename_columns', '{}')
    if rename_columns and rename_columns != '{}':
        try:
            rename_map = json.loads(rename_columns)
            for old_name, new_name in rename_map.items():
                if old_name in df.columns:
                    logger.info("Renaming column '%s' -> '%s'", old_name, new_name)
                    df = df.withColumnRenamed(old_name, new_name)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid rename-columns JSON: %s", exc)

    date_column = get_arg(args, 'date_column', '')
    if date_column and date_column in df.columns:
        logger.info("Parsing date column: %s", date_column)
        df = df.withColumn(date_column, F.to_date(F.col(date_column), 'yyyy-MM-dd'))

    initial_count = df.count()
    df = df.dropna(how='all')
    dropped_count = initial_count - df.count()
    if dropped_count > 0:
        logger.info("Dropped %d all-null rows", dropped_count)

    initial_count = df.count()
    df = df.distinct()
    dup_count = initial_count - df.count()
    if dup_count > 0:
        logger.info("Removed %d duplicate rows", dup_count)

    return df


def aggregate_transform(df, args: dict):
    """Apply aggregation transformation."""
    logger.info("Applying aggregate transformation")

    aggregate_column = get_arg(args, 'aggregate_column', '')
    if not aggregate_column:
        logger.warning("No aggregate-column specified, skipping aggregation")
        return df

    group_cols = [c for c in df.columns if c != aggregate_column and c != '_corrupt_record']
    if not group_cols:
        logger.warning("No grouping columns found, skipping aggregation")
        return df

    agg_func_map = {
        'sum': F.sum, 'avg': F.avg, 'count': F.count, 'min': F.min, 'max': F.max,
    }
    aggregate_func = get_arg(args, 'aggregate_func', 'sum')
    agg_func = agg_func_map.get(aggregate_func, F.sum)

    logger.info("Grouping by %s, applying %s on %s", group_cols, aggregate_func, aggregate_column)
    df = df.groupBy(*group_cols).agg(
        agg_func(F.col(aggregate_column)).alias(f"{aggregate_column}_{aggregate_func}")
    )
    return df


def passthrough_transform(df, args: dict):
    """No transformation, pass through as-is."""
    logger.info("Applying passthrough transformation (no changes)")
    return df


# ---------------------------------------------------------------------------
# Data writing
# ---------------------------------------------------------------------------

def write_output(df, output_path: str, args: dict) -> str:
    """Write the DataFrame to S3 in Parquet format."""
    num_partitions = int(get_arg(args, 'num_partitions', '1'))
    if num_partitions > 1:
        logger.info("Repartitioning to %d partitions", num_partitions)
        df = df.repartition(num_partitions)

    write_path = output_path.rstrip('/')

    writer = df.write \
        .format('parquet') \
        .mode('overwrite') \
        .option('compression', 'snappy')

    partition_by = get_arg(args, 'partition_by', '')
    if partition_by:
        partition_cols = [c.strip() for c in partition_by.split(',')]
        valid_partition_cols = [c for c in partition_cols if c in df.columns]
        if valid_partition_cols:
            logger.info("Partitioning by: %s", valid_partition_cols)
            writer = writer.partitionBy(*valid_partition_cols)

    logger.info("Writing output to: %s", write_path)
    writer.save(write_path)

    record_count = df.count()
    logger.info("Successfully wrote %d records to %s", record_count, write_path)
    return write_path


# ---------------------------------------------------------------------------
# Folder Lock Release
# ---------------------------------------------------------------------------

def _release_folder_lock(folder_prefix: str, environment: str) -> None:
    """
    Release the folder-level distributed lock after successful ETL completion.

    The lock was acquired by the Lambda when the _COMPLETE sentinel was detected.
    Releasing it here (after Parquet write + manifest + dedup marker) allows the
    same folder to be re-processed if needed (e.g., data correction, re-ingestion).

    If the Glue job fails, the lock is NOT released — this prevents the SQS
    retry from triggering a duplicate run. The lock will eventually expire via
    the S3 lifecycle policy (default: 7 days) or can be manually deleted.

    Lock key format: _locks/{sha256(folder_prefix)}.lock
    """
    if not folder_prefix:
        logger.info("No folder prefix provided — skipping lock release")
        return

    import hashlib
    folder_hash = hashlib.sha256(folder_prefix.encode()).hexdigest()
    lock_key = f"_locks/{folder_hash}.lock"

    # The processed bucket is also used as the lock bucket
    processed_bucket = os.environ.get('PROCESSED_BUCKET', '')

    if not processed_bucket:
        logger.warning(
            "PROCESSED_BUCKET env var not set — cannot release lock for folder '%s'",
            folder_prefix,
        )
        return

    try:
        import boto3
        s3_client = boto3.client('s3')
        s3_client.delete_object(Bucket=processed_bucket, Key=lock_key)
        logger.info(
            "Released folder lock: s3://%s/%s (folder: '%s')",
            processed_bucket, lock_key, folder_prefix,
        )
    except Exception as exc:
        # Lock release failure is non-fatal — data is already written
        logger.warning(
            "Failed to release folder lock s3://%s/%s: %s. "
            "Lock will expire via S3 lifecycle policy.",
            processed_bucket, lock_key, exc,
        )


# ---------------------------------------------------------------------------
# Glue Crawler Trigger
# ---------------------------------------------------------------------------

def _trigger_crawler(environment: str) -> None:
    """
    Trigger the Glue Crawler to update the Data Catalog schema after
    new Parquet data has been written to the processed bucket.

    The crawler scans the processed S3 prefix, infers the schema from
    the Parquet files, and updates the Glue Catalog table. This ensures
    Athena, Redshift Spectrum, and EMR always see the latest schema.

    The crawler runs asynchronously — we fire and forget. The crawler
    typically completes in 1-5 minutes depending on data volume.
    """
    crawler_name = f'{environment}-processed-data-crawler'

    try:
        import boto3
        glue_client = boto3.client('glue', region_name='us-east-1')

        # Check if crawler is already running
        crawler_info = glue_client.get_crawler(Name=crawler_name)
        crawler_state = crawler_info.get('Crawler', {}).get('State', 'UNKNOWN')

        if crawler_state == 'RUNNING':
            logger.info(
                "Crawler '%s' is already running — skipping trigger",
                crawler_name,
            )
            return

        glue_client.start_crawler(Name=crawler_name)
        logger.info(
            "Triggered Glue Crawler '%s' to update catalog schema",
            crawler_name,
        )

    except glue_client.exceptions.CrawlerRunningException:
        logger.info(
            "Crawler '%s' is already running — skipping trigger",
            crawler_name,
        )
    except Exception as exc:
        # Crawler trigger failure is non-fatal — the ETL data is already written
        logger.warning(
            "Failed to trigger Glue Crawler '%s': %s. "
            "Data is written but catalog schema may be stale until next crawl.",
            crawler_name, exc,
        )


# ---------------------------------------------------------------------------
# Metrics / Manifest
# ---------------------------------------------------------------------------

def write_manifest(metrics: dict, output_path: str, spark: SparkSession) -> None:
    """Write a JSON manifest file with job metrics."""
    manifest_path = f"{output_path.rstrip('/')}/_MANIFEST.json"
    metrics_json = json.dumps(metrics, indent=2, default=str)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(manifest_path)
    os = fs.create(path)
    os.write(metrics_json.encode('utf-8'))
    os.close()
    logger.info("Wrote manifest to: %s", manifest_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Main ETL job entry point with idempotency support and folder-level processing."""
    args = parse_args()
    job_start = datetime.utcnow()

    input_path = get_arg(args, 'input_path', '')
    input_paths_json = get_arg(args, 'input_paths', '')     # NEW: folder-level multi-file input
    output_path = get_arg(args, 'output_path', '')
    job_id = get_arg(args, 'job_id', 'unknown')
    environment = get_arg(args, 'environment', 'dev')
    file_dedup_key = get_arg(args, 'file_dedup_key', '')
    folder_prefix = get_arg(args, 'folder_prefix', '')      # NEW: folder prefix for logging
    transform_type = get_arg(args, 'transform', 'clean')
    delimiter = get_arg(args, 'delimiter', ',')
    header = get_arg(args, 'header', 'true')
    infer_schema = get_arg(args, 'infer_schema', 'true')

    # Determine processing mode
    is_folder_mode = bool(input_paths_json)
    input_desc = (
        f"folder '{folder_prefix}' ({len(json.loads(input_paths_json))} files)"
        if is_folder_mode and folder_prefix
        else f"folder ({len(json.loads(input_paths_json))} files)"
        if is_folder_mode
        else input_path
    )

    logger.info("=" * 60)
    logger.info("Starting Glue ETL Job: %s", job_id)
    logger.info("Environment: %s", environment)
    logger.info("Mode: %s", 'FOLDER' if is_folder_mode else 'SINGLE FILE')
    logger.info("Input: %s", input_desc)
    logger.info("Output: %s", output_path)
    logger.info("Transform: %s", transform_type)
    logger.info("File Dedup Key: %s", file_dedup_key or '(not provided)')
    logger.info("=" * 60)

    spark = create_spark_session(f"Glue-CSV-ETL-{job_id}")

    try:
        # ================================================================
        # IDEMPOTENCY CHECK
        # ================================================================
        if _is_already_processed(spark, output_path, file_dedup_key):
            logger.info("IDEMPOTENCY: %s already processed. Exiting successfully.", input_desc)
            job_end = datetime.utcnow()
            metrics = {
                'job_id': job_id,
                'environment': environment,
                'input_path': input_desc,
                'output_path': output_path,
                'transform': transform_type,
                'input_record_count': 0,
                'output_record_count': 0,
                'start_time': job_start.isoformat(),
                'end_time': job_end.isoformat(),
                'duration_seconds': (job_end - job_start).total_seconds(),
                'status': 'SKIPPED_DUPLICATE',
                'dedup_key': file_dedup_key,
                'mode': 'folder' if is_folder_mode else 'single',
            }
            write_manifest(metrics, output_path, spark)
            spark.stop()
            return

        # Step 1: Read CSV (single file or folder of files)
        if is_folder_mode:
            df = read_csv_files(
                spark=spark,
                input_paths_json=input_paths_json,
                delimiter=delimiter,
                header=header.lower() == 'true',
                infer_schema=infer_schema.lower() == 'true',
            )
        else:
            df = read_csv(
                spark=spark,
                input_path=input_path,
                delimiter=delimiter,
                header=header.lower() == 'true',
                infer_schema=infer_schema.lower() == 'true',
            )
        input_record_count = df.count()

        # Step 2: Apply transformation
        transform_map = {
            'clean': clean_transform,
            'aggregate': aggregate_transform,
            'passthrough': passthrough_transform,
        }
        transform_func = transform_map.get(transform_type, clean_transform)
        df_transformed = transform_func(df, args)
        output_record_count = df_transformed.count()

        # Step 3: Write output
        write_output(df=df_transformed, output_path=output_path, args=args)

        # Step 4: Collect metrics
        job_end = datetime.utcnow()
        duration_seconds = (job_end - job_start).total_seconds()

        metrics = {
            'job_id': job_id,
            'environment': environment,
            'input_path': input_path,
            'output_path': output_path,
            'transform': transform_type,
            'input_record_count': input_record_count,
            'output_record_count': output_record_count,
            'columns': df_transformed.columns,
            'start_time': job_start.isoformat(),
            'end_time': job_end.isoformat(),
            'duration_seconds': duration_seconds,
            'status': 'SUCCESS',
        }

        write_manifest(metrics, output_path, spark)
        _write_processed_marker(spark, output_path, file_dedup_key)

        # Step 5: Release folder-level lock (allows re-processing if needed)
        _release_folder_lock(folder_prefix, environment)

        # Step 6: Trigger Glue Crawler to update catalog schema
        _trigger_crawler(environment)

        logger.info("=" * 60)
        logger.info("Job %s completed successfully", job_id)
        logger.info("Duration: %.2f seconds", duration_seconds)
        logger.info("Input records: %d", input_record_count)
        logger.info("Output records: %d", output_record_count)
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("Job %s failed: %s", job_id, exc, exc_info=True)
        job_end = datetime.utcnow()
        duration_seconds = (job_end - job_start).total_seconds()

        metrics = {
            'job_id': job_id,
            'environment': environment,
            'input_path': input_path,
            'start_time': job_start.isoformat(),
            'end_time': job_end.isoformat(),
            'duration_seconds': duration_seconds,
            'status': 'FAILED',
            'error': str(exc),
        }

        try:
            write_manifest(metrics, output_path, spark)
        except Exception:
            logger.warning("Failed to write error manifest")

        sys.exit(1)

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
