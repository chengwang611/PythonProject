#!/usr/bin/env python3
"""
PySpark ETL Job for CSV Processing on Amazon EMR (Idempotent)

This script is submitted as an EMR step by the Lambda trigger function.
It reads CSV files from S3, performs transformations, and writes the
processed output back to S3 in Parquet format (with optional partitioning).

IDEMPOTENCY: This job checks for an existing output marker before processing.
If the output already exists (from a previous run), the job exits early.
This provides a second line of defense for exactly-once processing.

Usage (submitted via EMR step):
    spark-submit csv_etl_job.py \\
        --input-path s3://bucket/path/file.csv \\
        --output-path s3://bucket/output/job-id/ \\
        --job-id abc123 \\
        --environment dev

Optional arguments:
    --delimiter          CSV delimiter (default: comma)
    --header             Whether CSV has header (default: true)
    --infer-schema       Infer schema from CSV (default: true)
    --partition-by       Column(s) to partition output by (e.g., "year,month")
    --num-partitions     Number of output partitions (default: 1)
    --transform          Transformation type: clean, aggregate, passthrough (default: clean)
    --aggregate-column   Column to aggregate on (for aggregate transform)
    --aggregate-func     Aggregation function: sum, avg, count, min, max (default: sum)
    --drop-columns       Comma-separated columns to drop
    --rename-columns     JSON map of old->new column names
    --date-column        Column to parse as date for partitioning
    --file-dedup-key     SHA-256 hash of the input file path for dedup (optional)
"""

import argparse
import json
import logging
import sys
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
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='PySpark CSV ETL Job for EMR')
    parser.add_argument('--input-path', required=True, help='S3 path to input CSV file')
    parser.add_argument('--output-path', required=True, help='S3 path for output')
    parser.add_argument('--job-id', required=True, help='Unique job identifier')
    parser.add_argument('--environment', default='dev', help='Environment name')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter')
    parser.add_argument('--header', default='true', choices=['true', 'false'], help='CSV has header')
    parser.add_argument('--infer-schema', default='true', choices=['true', 'false'], help='Infer schema')
    parser.add_argument('--partition-by', default='', help='Column(s) to partition output by')
    parser.add_argument('--num-partitions', type=int, default=1, help='Number of output partitions')
    parser.add_argument('--transform', default='clean', choices=['clean', 'aggregate', 'passthrough'],
                        help='Transformation type')
    parser.add_argument('--aggregate-column', default='', help='Column to aggregate')
    parser.add_argument('--aggregate-func', default='sum', choices=['sum', 'avg', 'count', 'min', 'max'],
                        help='Aggregation function')
    parser.add_argument('--drop-columns', default='', help='Comma-separated columns to drop')
    parser.add_argument('--rename-columns', default='{}', help='JSON map of old->new column names')
    parser.add_argument('--date-column', default='', help='Column to parse as date')
    parser.add_argument('--file-dedup-key', default='', help='SHA-256 hash of input path for dedup')
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure a Spark session."""
    builder = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')

    # S3A configuration for EMR
    builder = builder \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.fast.upload', 'true')

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Data reading
# ---------------------------------------------------------------------------
def _is_already_processed(spark: SparkSession, output_path: str, file_dedup_key: str) -> bool:
    """
    Idempotency check: verify if this file has already been processed.

    Checks for a _SUCCESS marker file at a deterministic location based on
    the file's dedup key. This is a second line of defense after the
    DynamoDB check in the Lambda function.

    Args:
        spark: SparkSession
        output_path: Base output path
        file_dedup_key: SHA-256 hash of the input file path

    Returns:
        True if the file was already processed, False otherwise.
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
    """
    Write a _SUCCESS marker file to indicate this file was processed.

    This marker is used by future idempotency checks to skip already-processed files.
    """
    if not file_dedup_key:
        return

    marker_path = f"{output_path.rstrip('/')}/_dedup_markers/{file_dedup_key}/_SUCCESS"

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(marker_path)

        # Create parent directories and write marker
        if not fs.exists(path.getParent()):
            fs.mkdirs(path.getParent())

        fs.create(path).close()
        logger.info("Wrote idempotency marker: %s", marker_path)

    except Exception as exc:
        logger.warning("Failed to write idempotency marker: %s", exc)


def read_csv(spark: SparkSession, input_path: str, delimiter: str,
             header: bool, infer_schema: bool) -> 'DataFrame':
    """
    Read a CSV file from S3 into a Spark DataFrame.

    Args:
        spark: SparkSession
        input_path: S3 path to CSV file
        delimiter: CSV delimiter character
        header: Whether the CSV has a header row
        infer_schema: Whether to infer column types

    Returns:
        Spark DataFrame
    """
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
    logger.info(
        "Loaded %d records with %d columns from %s",
        record_count, column_count, input_path,
    )

    return df


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------
def clean_transform(df: 'DataFrame', args: argparse.Namespace) -> 'DataFrame':
    """
    Apply cleaning transformations:
    - Drop corrupt records
    - Drop specified columns
    - Rename columns
    - Parse date columns
    - Drop rows with all nulls
    - Deduplicate
    """
    logger.info("Applying clean transformation")

    # Drop corrupt records
    if '_corrupt_record' in df.columns:
        corrupt_count = df.filter(F.col('_corrupt_record').isNotNull()).count()
        if corrupt_count > 0:
            logger.warning("Dropping %d corrupt records", corrupt_count)
        df = df.filter(F.col('_corrupt_record').isNull()).drop('_corrupt_record')

    # Drop specified columns
    if args.drop_columns:
        drop_cols = [c.strip() for c in args.drop_columns.split(',')]
        existing_drop = [c for c in drop_cols if c in df.columns]
        if existing_drop:
            logger.info("Dropping columns: %s", existing_drop)
            df = df.drop(*existing_drop)

    # Rename columns
    if args.rename_columns and args.rename_columns != '{}':
        try:
            rename_map = json.loads(args.rename_columns)
            for old_name, new_name in rename_map.items():
                if old_name in df.columns:
                    logger.info("Renaming column '%s' -> '%s'", old_name, new_name)
                    df = df.withColumnRenamed(old_name, new_name)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid rename-columns JSON: %s", exc)

    # Parse date column
    if args.date_column and args.date_column in df.columns:
        logger.info("Parsing date column: %s", args.date_column)
        df = df.withColumn(
            args.date_column,
            F.to_date(F.col(args.date_column), 'yyyy-MM-dd')
        )

    # Drop rows where all columns are null
    initial_count = df.count()
    df = df.dropna(how='all')
    dropped_count = initial_count - df.count()
    if dropped_count > 0:
        logger.info("Dropped %d all-null rows", dropped_count)

    # Deduplicate
    initial_count = df.count()
    df = df.distinct()
    dup_count = initial_count - df.count()
    if dup_count > 0:
        logger.info("Removed %d duplicate rows", dup_count)

    return df


def aggregate_transform(df: 'DataFrame', args: argparse.Namespace) -> 'DataFrame':
    """
    Apply aggregation transformation.
    Groups by all non-numeric columns and applies the specified aggregation.
    """
    logger.info("Applying aggregate transformation")

    if not args.aggregate_column:
        logger.warning("No aggregate-column specified, skipping aggregation")
        return df

    # Identify grouping columns (all columns except the aggregate column)
    group_cols = [c for c in df.columns if c != args.aggregate_column and c != '_corrupt_record']

    if not group_cols:
        logger.warning("No grouping columns found, skipping aggregation")
        return df

    # Map aggregation function name to PySpark function
    agg_func_map = {
        'sum': F.sum,
        'avg': F.avg,
        'count': F.count,
        'min': F.min,
        'max': F.max,
    }

    agg_func = agg_func_map.get(args.aggregate_func, F.sum)
    logger.info(
        "Grouping by %s, applying %s on %s",
        group_cols, args.aggregate_func, args.aggregate_column,
    )

    df = df.groupBy(*group_cols).agg(
        agg_func(F.col(args.aggregate_column)).alias(f"{args.aggregate_column}_{args.aggregate_func}")
    )

    return df


def passthrough_transform(df: 'DataFrame', args: argparse.Namespace) -> 'DataFrame':
    """No transformation, pass through as-is."""
    logger.info("Applying passthrough transformation (no changes)")
    return df


# ---------------------------------------------------------------------------
# Data writing
# ---------------------------------------------------------------------------
def write_output(df: 'DataFrame', output_path: str, args: argparse.Namespace) -> str:
    """
    Write the DataFrame to S3 in Parquet format.

    Args:
        df: Spark DataFrame to write
        output_path: S3 output path
        args: Parsed command-line arguments

    Returns:
        Full output path written to
    """
    # Repartition if needed
    if args.num_partitions > 1:
        logger.info("Repartitioning to %d partitions", args.num_partitions)
        df = df.repartition(args.num_partitions)

    # Determine write mode
    write_path = output_path.rstrip('/')

    writer = df.write \
        .format('parquet') \
        .mode('overwrite') \
        .option('compression', 'snappy')

    # Partition by specified columns
    if args.partition_by:
        partition_cols = [c.strip() for c in args.partition_by.split(',')]
        valid_partition_cols = [c for c in partition_cols if c in df.columns]
        if valid_partition_cols:
            logger.info("Partitioning by: %s", valid_partition_cols)
            writer = writer.partitionBy(*valid_partition_cols)

    logger.info("Writing output to: %s", write_path)
    writer.save(write_path)

    # Write a _SUCCESS marker
    df.sparkSession.sparkContext._jsc.hadoopConfiguration().set(
        'mapreduce.fileoutputcommitter.marksuccessfuljobs', 'true'
    )

    record_count = df.count()
    logger.info("Successfully wrote %d records to %s", record_count, write_path)

    return write_path


# ---------------------------------------------------------------------------
# Metrics / Manifest
# ---------------------------------------------------------------------------
def write_manifest(metrics: dict, output_path: str, spark: SparkSession) -> None:
    """
    Write a JSON manifest file with job metrics to the output location.
    """
    manifest_path = f"{output_path.rstrip('/')}/_MANIFEST.json"
    metrics_json = json.dumps(metrics, indent=2, default=str)

    # Use Hadoop filesystem to write the manifest
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
    """Main ETL job entry point with idempotency support."""
    args = parse_args()
    job_start = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("Starting ETL Job: %s", args.job_id)
    logger.info("Environment: %s", args.environment)
    logger.info("Input: %s", args.input_path)
    logger.info("Output: %s", args.output_path)
    logger.info("Transform: %s", args.transform)
    logger.info("File Dedup Key: %s", args.file_dedup_key or '(not provided)')
    logger.info("=" * 60)

    # Create Spark session
    spark = create_spark_session(f"CSV-ETL-{args.job_id}")
    spark.sparkContext.setLogLevel('WARN')

    try:
        # ================================================================
        # IDEMPOTENCY CHECK: Skip if already processed
        # ================================================================
        if _is_already_processed(spark, args.output_path, args.file_dedup_key):
            logger.info(
                "IDEMPOTENCY: File %s already processed. Exiting successfully.",
                args.input_path,
            )
            job_end = datetime.utcnow()
            metrics = {
                'job_id': args.job_id,
                'environment': args.environment,
                'input_path': args.input_path,
                'output_path': args.output_path,
                'transform': args.transform,
                'input_record_count': 0,
                'output_record_count': 0,
                'start_time': job_start.isoformat(),
                'end_time': job_end.isoformat(),
                'duration_seconds': (job_end - job_start).total_seconds(),
                'status': 'SKIPPED_DUPLICATE',
                'dedup_key': args.file_dedup_key,
            }
            write_manifest(metrics, args.output_path, spark)
            spark.stop()
            return

        # Step 1: Read CSV
        df = read_csv(
            spark=spark,
            input_path=args.input_path,
            delimiter=args.delimiter,
            header=args.header.lower() == 'true',
            infer_schema=args.infer_schema.lower() == 'true',
        )

        input_record_count = df.count()

        # Step 2: Apply transformation
        transform_map = {
            'clean': clean_transform,
            'aggregate': aggregate_transform,
            'passthrough': passthrough_transform,
        }

        transform_func = transform_map.get(args.transform, clean_transform)
        df_transformed = transform_func(df, args)

        output_record_count = df_transformed.count()

        # Step 3: Write output
        output_path = write_output(
            df=df_transformed,
            output_path=args.output_path,
            args=args,
        )

        # Step 4: Collect metrics
        job_end = datetime.utcnow()
        duration_seconds = (job_end - job_start).total_seconds()

        metrics = {
            'job_id': args.job_id,
            'environment': args.environment,
            'input_path': args.input_path,
            'output_path': output_path,
            'transform': args.transform,
            'input_record_count': input_record_count,
            'output_record_count': output_record_count,
            'columns': df_transformed.columns,
            'start_time': job_start.isoformat(),
            'end_time': job_end.isoformat(),
            'duration_seconds': duration_seconds,
            'status': 'SUCCESS',
        }

        # Write manifest
        write_manifest(metrics, args.output_path, spark)

        # Write idempotency marker (second line of defense)
        _write_processed_marker(spark, args.output_path, args.file_dedup_key)

        logger.info("=" * 60)
        logger.info("Job %s completed successfully", args.job_id)
        logger.info("Duration: %.2f seconds", duration_seconds)
        logger.info("Input records: %d", input_record_count)
        logger.info("Output records: %d", output_record_count)
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("Job %s failed: %s", args.job_id, exc, exc_info=True)
        job_end = datetime.utcnow()
        duration_seconds = (job_end - job_start).total_seconds()

        metrics = {
            'job_id': args.job_id,
            'environment': args.environment,
            'input_path': args.input_path,
            'start_time': job_start.isoformat(),
            'end_time': job_end.isoformat(),
            'duration_seconds': duration_seconds,
            'status': 'FAILED',
            'error': str(exc),
        }

        try:
            write_manifest(metrics, args.output_path, spark)
        except Exception:
            logger.warning("Failed to write error manifest")

        sys.exit(1)

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
