"""
AWS Glue 5.x PySpark CDC Apply Job
==================================

Purpose
-------
Read CDC records from two Iceberg staging tables:

    1. history staging
    2. streaming staging

Normalize and combine the records, deterministically select one winning change
per business key, and apply INSERT / UPDATE / DELETE changes to one target
Iceberg table by using Spark SQL MERGE INTO.

Expected canonical staging columns
----------------------------------
The history and streaming staging tables should expose the same canonical CDC
schema. At minimum:

    business-key column(s)       Example: customer_id
    operation column             I / U / D, or C / R / U / D
    sequence column(s)           Example: source_lsn, event_sequence
    ingestion timestamp          Example: ingested_at

All target business columns should also exist in the staging tables.

Important production assumptions
--------------------------------
1. The target Iceberg table already exists.
2. Staging tables and target table are registered in AWS Glue Data Catalog.
3. The Glue job role has the required IAM and Lake Formation permissions.
4. Job parameters configure the Iceberg Glue catalog, for example:

   --datalake-formats iceberg
   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
   --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
   --conf spark.sql.catalog.glue_catalog.warehouse=s3://YOUR-WAREHOUSE/
   --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
   --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO

5. Each run should normally receive predicates identifying uncommitted staging
   batches. Without predicates, the job reads all staging data. The MERGE is
   designed to be idempotent, but rereading all history is inefficient.

Required Glue arguments
-----------------------
--JOB_NAME
--HISTORY_TABLE
--STREAM_TABLE
--TARGET_TABLE
--BUSINESS_KEYS
--SEQUENCE_COLUMNS
--OPERATION_COLUMN
--INGESTION_TS_COLUMN

Optional Glue arguments
-----------------------
--HISTORY_FILTER
--STREAM_FILTER
--SOURCE_COLUMN
--SOURCE_PRIORITY_COLUMN
--DELETE_VALUES
--INSERT_VALUES
--UPDATE_VALUES
--ENABLE_TARGET_VERSION_GUARD
--AUDIT_OUTPUT_PATH
--FAIL_ON_EMPTY_INPUT
--RUN_ID
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger("cdc_apply_job")
LOGGER.setLevel(logging.INFO)


def log_json(event: str, **values: object) -> None:
    """
    Write one structured JSON event.

    Glue sends stdout/stderr and Python logging output to CloudWatch Logs.
    Structured JSON makes CloudWatch Logs Insights queries easier.
    """
    payload = {
        "event": event,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        **values,
    }

    LOGGER.info(
        json.dumps(
            payload,
            default=str,
            sort_keys=True,
        )
    )


# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JobConfig:
    job_name: str

    history_table: str
    stream_table: str
    target_table: str

    business_keys: Tuple[str, ...]
    sequence_columns: Tuple[str, ...]

    operation_column: str
    ingestion_ts_column: str

    history_filter: Optional[str]
    stream_filter: Optional[str]

    source_column: str
    source_priority_column: str

    delete_values: Tuple[str, ...]
    insert_values: Tuple[str, ...]
    update_values: Tuple[str, ...]

    enable_target_version_guard: bool
    audit_output_path: Optional[str]
    fail_on_empty_input: bool

    run_id: str


REQUIRED_ARGS = [
    "JOB_NAME",
    "HISTORY_TABLE",
    "STREAM_TABLE",
    "TARGET_TABLE",
    "BUSINESS_KEYS",
    "SEQUENCE_COLUMNS",
    "OPERATION_COLUMN",
    "INGESTION_TS_COLUMN",
]


OPTIONAL_ARG_NAMES = {
    "HISTORY_FILTER",
    "STREAM_FILTER",
    "SOURCE_COLUMN",
    "SOURCE_PRIORITY_COLUMN",
    "DELETE_VALUES",
    "INSERT_VALUES",
    "UPDATE_VALUES",
    "ENABLE_TARGET_VERSION_GUARD",
    "AUDIT_OUTPUT_PATH",
    "FAIL_ON_EMPTY_INPUT",
    "RUN_ID",
}


def present_optional_args(argv: Sequence[str]) -> List[str]:
    """
    Return optional argument names actually present in sys.argv.

    getResolvedOptions raises an exception when an argument is requested
    but was not supplied, so optional arguments must first be detected.
    """
    present: List[str] = []
    tokens = set(argv)

    for name in OPTIONAL_ARG_NAMES:
        if f"--{name}" in tokens:
            present.append(name)

    return present


def csv_tuple(value: str) -> Tuple[str, ...]:
    """
    Convert a comma-separated parameter into a non-empty tuple.
    """
    result = tuple(
        item.strip()
        for item in value.split(",")
        if item.strip()
    )

    if not result:
        raise ValueError(
            f"Expected a non-empty comma-separated value: {value!r}"
        )

    return result


def bool_value(value: str) -> bool:
    """
    Parse a Glue string argument as a boolean.
    """
    normalized = value.strip().lower()

    if normalized in {"true", "1", "yes", "y"}:
        return True

    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(
        f"Invalid boolean value: {value!r}"
    )


def parse_config(argv: Sequence[str]) -> JobConfig:
    """
    Parse required and optional Glue arguments.
    """
    optional_present = present_optional_args(argv)

    args = getResolvedOptions(
        list(argv),
        REQUIRED_ARGS + optional_present,
    )

    return JobConfig(
        job_name=args["JOB_NAME"],

        history_table=args["HISTORY_TABLE"],
        stream_table=args["STREAM_TABLE"],
        target_table=args["TARGET_TABLE"],

        business_keys=csv_tuple(
            args["BUSINESS_KEYS"]
        ),

        sequence_columns=csv_tuple(
            args["SEQUENCE_COLUMNS"]
        ),

        operation_column=args["OPERATION_COLUMN"],
        ingestion_ts_column=args["INGESTION_TS_COLUMN"],

        history_filter=args.get("HISTORY_FILTER"),
        stream_filter=args.get("STREAM_FILTER"),

        source_column=args.get(
            "SOURCE_COLUMN",
            "source_type",
        ),

        source_priority_column=args.get(
            "SOURCE_PRIORITY_COLUMN",
            "source_priority",
        ),

        delete_values=tuple(
            value.upper()
            for value in csv_tuple(
                args.get(
                    "DELETE_VALUES",
                    "D,DELETE",
                )
            )
        ),

        insert_values=tuple(
            value.upper()
            for value in csv_tuple(
                args.get(
                    "INSERT_VALUES",
                    "I,C,R,INSERT,CREATE,READ",
                )
            )
        ),

        update_values=tuple(
            value.upper()
            for value in csv_tuple(
                args.get(
                    "UPDATE_VALUES",
                    "U,UPDATE",
                )
            )
        ),

        enable_target_version_guard=bool_value(
            args.get(
                "ENABLE_TARGET_VERSION_GUARD",
                "true",
            )
        ),

        audit_output_path=args.get(
            "AUDIT_OUTPUT_PATH"
        ),

        fail_on_empty_input=bool_value(
            args.get(
                "FAIL_ON_EMPTY_INPUT",
                "false",
            )
        ),

        run_id=args.get(
            "RUN_ID",
            str(uuid.uuid4()),
        ),
    )


# ---------------------------------------------------------------------------
# Spark SQL identifier helpers
# ---------------------------------------------------------------------------

def quote_identifier(name: str) -> str:
    """
    Quote one Spark SQL identifier.

    Example:
        customer_id -> `customer_id`
    """
    return f"`{name.replace('`', '``')}`"


def quote_table_name(full_name: str) -> str:
    """
    Quote a multipart catalog.database.table name.

    Example:
        glue_catalog.curated.customer
        ->
        `glue_catalog`.`curated`.`customer`
    """
    parts = [
        part.strip()
        for part in full_name.split(".")
    ]

    if not all(parts):
        raise ValueError(
            f"Invalid table name: {full_name!r}"
        )

    return ".".join(
        quote_identifier(part)
        for part in parts
    )


def sql_string(value: str) -> str:
    """
    Safely quote a SQL string literal.
    """
    return "'" + value.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Schema validation and stage reads
# ---------------------------------------------------------------------------

def require_columns(
    df: DataFrame,
    required: Iterable[str],
    *,
    table_name: str,
) -> None:
    """
    Fail early when a staging table is missing required CDC columns.
    """
    missing = sorted(
        set(required) - set(df.columns)
    )

    if missing:
        raise ValueError(
            f"Table {table_name} is missing required columns: {missing}"
        )


def read_stage(
    spark: SparkSession,
    table_name: str,
    predicate: Optional[str],
    source_name: str,
    config: JobConfig,
) -> DataFrame:
    """
    Read one Iceberg staging table.

    The stage table is expected to be append-only and contain canonical
    CDC records.

    Default source priority:
        STREAM  = 200
        HISTORY = 100

    A staging row may override this by providing source_priority.
    """
    df = spark.table(table_name)

    required_columns = (
        list(config.business_keys)
        + list(config.sequence_columns)
        + [
            config.operation_column,
            config.ingestion_ts_column,
        ]
    )

    require_columns(
        df,
        required_columns,
        table_name=table_name,
    )

    if predicate:
        # This predicate should come from trusted deployment/orchestration
        # configuration, for example:
        #
        # stream_batch_id BETWEEN 1000 AND 1050
        #
        # It should not come directly from an end user.
        df = df.where(predicate)

    default_priority = (
        200
        if source_name == "STREAM"
        else 100
    )

    if config.source_column in df.columns:
        df = df.withColumn(
            config.source_column,
            F.coalesce(
                F.col(
                    config.source_column
                ).cast("string"),
                F.lit(source_name),
            ),
        )
    else:
        df = df.withColumn(
            config.source_column,
            F.lit(source_name),
        )

    if config.source_priority_column in df.columns:
        df = df.withColumn(
            config.source_priority_column,
            F.coalesce(
                F.col(
                    config.source_priority_column
                ).cast("long"),
                F.lit(default_priority),
            ),
        )
    else:
        df = df.withColumn(
            config.source_priority_column,
            F.lit(
                default_priority
            ).cast("long"),
        )

    # Normalize CDC operation values.
    df = df.withColumn(
        config.operation_column,
        F.upper(
            F.trim(
                F.col(
                    config.operation_column
                ).cast("string")
            )
        ),
    )

    valid_operations = set(
        config.delete_values
        + config.insert_values
        + config.update_values
    )

    invalid_operations = (
        df.select(
            config.operation_column
        )
        .where(
            F.col(
                config.operation_column
            ).isNull()
            |
            ~F.col(
                config.operation_column
            ).isin(
                sorted(valid_operations)
            )
        )
        .limit(20)
        .collect()
    )

    if invalid_operations:
        bad_values = [
            row[config.operation_column]
            for row in invalid_operations
        ]

        raise ValueError(
            f"Table {table_name} contains unsupported CDC operations: "
            f"{bad_values}. Supported values: "
            f"{sorted(valid_operations)}"
        )

    return df


def align_to_target(
    stage_df: DataFrame,
    target_df: DataFrame,
    config: JobConfig,
) -> DataFrame:
    """
    Align staging business columns with the target Iceberg schema.

    Behavior:
      * Existing staging columns are cast to target types.
      * Missing nullable target columns are added as null.
      * Missing key or sequence columns cause failure.
      * Extra staging control columns are retained.
    """
    result = stage_df

    target_fields: Dict[str, StructField] = {
        field.name: field
        for field in target_df.schema.fields
    }

    protected_columns = (
        set(config.business_keys)
        | set(config.sequence_columns)
    )

    for name, field in target_fields.items():
        if name in result.columns:
            result = result.withColumn(
                name,
                F.col(name).cast(
                    field.dataType
                ),
            )
        else:
            if name in protected_columns:
                raise ValueError(
                    f"Required target column {name!r} "
                    "is absent from staging data."
                )

            result = result.withColumn(
                name,
                F.lit(None).cast(
                    field.dataType
                ),
            )

    return result


# ---------------------------------------------------------------------------
# CDC ordering and deduplication
# ---------------------------------------------------------------------------

def build_order_columns(
    config: JobConfig,
) -> List:
    """
    Build deterministic ordering for selecting the winning CDC record.

    Priority:
      1. sequence columns descending
      2. source priority descending
      3. ingestion timestamp descending
      4. DELETE wins exact ties
    """
    order_columns = [
        F.col(column_name).desc_nulls_last()
        for column_name in config.sequence_columns
    ]

    order_columns.extend(
        [
            F.col(
                config.source_priority_column
            ).desc_nulls_last(),

            F.col(
                config.ingestion_ts_column
            ).desc_nulls_last(),

            F.when(
                F.col(
                    config.operation_column
                ).isin(
                    *config.delete_values
                ),
                F.lit(3),
            )
            .when(
                F.col(
                    config.operation_column
                ).isin(
                    *config.update_values
                ),
                F.lit(2),
            )
            .otherwise(
                F.lit(1)
            )
            .desc(),
        ]
    )

    return order_columns


def choose_winning_changes(
    combined_df: DataFrame,
    config: JobConfig,
) -> DataFrame:
    """
    Select exactly one incoming CDC row per business key.

    This is essential because Spark/Iceberg MERGE should not receive
    multiple source rows that match the same target row.
    """
    key_window = (
        Window
        .partitionBy(
            *[
                F.col(key)
                for key in config.business_keys
            ]
        )
        .orderBy(
            *build_order_columns(config)
        )
    )

    return (
        combined_df
        .withColumn(
            "__cdc_rank",
            F.row_number().over(
                key_window
            ),
        )
        .where(
            F.col("__cdc_rank") == 1
        )
        .drop("__cdc_rank")
    )


# ---------------------------------------------------------------------------
# Version comparison logic
# ---------------------------------------------------------------------------

def sequence_is_newer_condition(
    sequence_columns: Sequence[str],
    source_alias: str = "s",
    target_alias: str = "t",
) -> str:
    """
    Generate a lexicographic "source version is newer" SQL expression.

    Example sequence columns:
        source_lsn, event_sequence

    Generated logic is conceptually:

        s.source_lsn > t.source_lsn

        OR

        (
          s.source_lsn = t.source_lsn
          AND
          s.event_sequence > t.event_sequence
        )

    Null target versions are considered older than non-null source versions.
    """
    clauses: List[str] = []

    for index, current_name in enumerate(
        sequence_columns
    ):
        equal_prefix: List[str] = []

        for previous_name in sequence_columns[:index]:
            source_previous = (
                f"{source_alias}."
                f"{quote_identifier(previous_name)}"
            )

            target_previous = (
                f"{target_alias}."
                f"{quote_identifier(previous_name)}"
            )

            # Spark null-safe equality.
            equal_prefix.append(
                f"{source_previous} <=> {target_previous}"
            )

        source_current = (
            f"{source_alias}."
            f"{quote_identifier(current_name)}"
        )

        target_current = (
            f"{target_alias}."
            f"{quote_identifier(current_name)}"
        )

        current_is_greater = (
            f"("
            f"{target_current} IS NULL "
            f"AND {source_current} IS NOT NULL"
            f") "
            f"OR "
            f"("
            f"{source_current} > {target_current}"
            f")"
        )

        if equal_prefix:
            clauses.append(
                "("
                + " AND ".join(equal_prefix)
                + f" AND ({current_is_greater})"
                + ")"
            )
        else:
            clauses.append(
                f"({current_is_greater})"
            )

    if not clauses:
        raise ValueError(
            "At least one sequence column is required."
        )

    return (
        "("
        + " OR ".join(clauses)
        + ")"
    )


# ---------------------------------------------------------------------------
# MERGE SQL generation
# ---------------------------------------------------------------------------

def build_merge_sql(
    target_table: str,
    source_view: str,
    target_columns: Sequence[str],
    config: JobConfig,
) -> str:
    """
    Build an idempotent Iceberg MERGE INTO statement.

    Rules:
      * DELETE removes an existing target row only when source is newer.
      * UPDATE replaces an existing target row only when source is newer.
      * INSERT creates a missing target row unless the operation is DELETE.
      * Reprocessing the same source version does not change the result.
    """
    target = quote_table_name(
        target_table
    )

    source = quote_identifier(
        source_view
    )

    key_match = " AND ".join(
        (
            f"t.{quote_identifier(name)} "
            f"= "
            f"s.{quote_identifier(name)}"
        )
        for name in config.business_keys
    )

    delete_values_sql = ", ".join(
        sql_string(value)
        for value in config.delete_values
    )

    non_delete_values = (
        config.insert_values
        + config.update_values
    )

    non_delete_values_sql = ", ".join(
        sql_string(value)
        for value in non_delete_values
    )

    target_sequence_available = all(
        column in target_columns
        for column in config.sequence_columns
    )

    if (
        config.enable_target_version_guard
        and not target_sequence_available
    ):
        missing = [
            column
            for column in config.sequence_columns
            if column not in target_columns
        ]

        raise ValueError(
            "ENABLE_TARGET_VERSION_GUARD=true, "
            "but the target table does not contain "
            f"sequence columns {missing}. "
            "Add those columns to the target table or set "
            "ENABLE_TARGET_VERSION_GUARD=false."
        )

    newer_condition = (
        sequence_is_newer_condition(
            config.sequence_columns
        )
        if config.enable_target_version_guard
        else "TRUE"
    )

    update_columns = [
        name
        for name in target_columns
        if name not in config.business_keys
    ]

    if not update_columns:
        raise ValueError(
            "Target table has no non-key columns to update."
        )

    update_assignments = ",\n        ".join(
        (
            f"t.{quote_identifier(name)} "
            f"= "
            f"s.{quote_identifier(name)}"
        )
        for name in update_columns
    )

    insert_column_sql = ", ".join(
        quote_identifier(name)
        for name in target_columns
    )

    insert_value_sql = ", ".join(
        f"s.{quote_identifier(name)}"
        for name in target_columns
    )

    operation_expression = (
        f"s.{quote_identifier(config.operation_column)}"
    )

    return f"""
MERGE INTO {target} AS t
USING {source} AS s
ON {key_match}

WHEN MATCHED
  AND {operation_expression} IN ({delete_values_sql})
  AND {newer_condition}
THEN DELETE

WHEN MATCHED
  AND {operation_expression} IN ({non_delete_values_sql})
  AND {newer_condition}
THEN UPDATE SET
        {update_assignments}

WHEN NOT MATCHED
  AND {operation_expression} IN ({non_delete_values_sql})
THEN INSERT ({insert_column_sql})
     VALUES ({insert_value_sql})
""".strip()


# ---------------------------------------------------------------------------
# Optional audit output
# ---------------------------------------------------------------------------

def write_audit_record(
    spark: SparkSession,
    config: JobConfig,
    values: Dict[str, object],
) -> None:
    """
    Write a one-row JSON audit record to S3.

    This is optional. The audit path can be used by:
      * CloudWatch/Athena operational reporting
      * Airflow reconciliation
      * Merge control workflows
    """
    if not config.audit_output_path:
        return

    output_path = (
        config.audit_output_path.rstrip("/")
        + f"/run_id={config.run_id}/"
    )

    (
        spark
        .createDataFrame([values])
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(output_path)
    )


# ---------------------------------------------------------------------------
# Main Glue job
# ---------------------------------------------------------------------------

def main() -> None:
    started_at = time.time()

    config = parse_config(
        sys.argv
    )

    spark_context = (
        SparkContext.getOrCreate()
    )

    glue_context = GlueContext(
        spark_context
    )

    spark = (
        glue_context.spark_session
    )

    job = Job(
        glue_context
    )

    job.init(
        config.job_name,
        vars(config),
    )

    log_json(
        "cdc_merge_started",
        job_name=config.job_name,
        run_id=config.run_id,
        history_table=config.history_table,
        stream_table=config.stream_table,
        target_table=config.target_table,
        history_filter=config.history_filter,
        stream_filter=config.stream_filter,
        business_keys=config.business_keys,
        sequence_columns=config.sequence_columns,
    )

    try:
        # ---------------------------------------------------------------
        # 1. Read target schema.
        # ---------------------------------------------------------------
        target_df = spark.table(
            config.target_table
        )

        target_columns = (
            target_df.columns
        )

        # ---------------------------------------------------------------
        # 2. Read unprocessed history and stream staging ranges.
        # ---------------------------------------------------------------
        history_df = read_stage(
            spark=spark,
            table_name=config.history_table,
            predicate=config.history_filter,
            source_name="HISTORY",
            config=config,
        )

        stream_df = read_stage(
            spark=spark,
            table_name=config.stream_table,
            predicate=config.stream_filter,
            source_name="STREAM",
            config=config,
        )

        # ---------------------------------------------------------------
        # 3. Align both staging schemas with the target table.
        # ---------------------------------------------------------------
        history_df = align_to_target(
            history_df,
            target_df,
            config,
        )

        stream_df = align_to_target(
            stream_df,
            target_df,
            config,
        )

        # ---------------------------------------------------------------
        # 4. Combine both CDC streams.
        # ---------------------------------------------------------------
        combined_df = (
            history_df
            .unionByName(
                stream_df,
                allowMissingColumns=True,
            )
            .persist()
        )

        input_count = (
            combined_df.count()
        )

        if input_count == 0:
            message = (
                "No staging records matched "
                "the supplied filters."
            )

            log_json(
                "cdc_merge_empty",
                job_name=config.job_name,
                run_id=config.run_id,
                message=message,
            )

            if config.fail_on_empty_input:
                raise RuntimeError(
                    message
                )

            job.commit()
            return

        # ---------------------------------------------------------------
        # 5. Select one winning CDC row per business key.
        # ---------------------------------------------------------------
        winning_df = (
            choose_winning_changes(
                combined_df,
                config,
            )
            .persist()
        )

        winning_count = (
            winning_df.count()
        )

        duplicate_or_superseded_count = (
            input_count - winning_count
        )

        delete_count = (
            winning_df
            .where(
                F.col(
                    config.operation_column
                ).isin(
                    *config.delete_values
                )
            )
            .count()
        )

        upsert_count = (
            winning_count
            - delete_count
        )

        # ---------------------------------------------------------------
        # 6. Create a temporary source view for Spark SQL MERGE.
        # ---------------------------------------------------------------
        source_view = (
            "cdc_source_"
            + config.run_id.replace("-", "_")
        )

        winning_df.createOrReplaceTempView(
            source_view
        )

        # ---------------------------------------------------------------
        # 7. Generate the deterministic MERGE statement.
        # ---------------------------------------------------------------
        merge_sql = build_merge_sql(
            target_table=config.target_table,
            source_view=source_view,
            target_columns=target_columns,
            config=config,
        )

        log_json(
            "cdc_merge_prepared",
            job_name=config.job_name,
            run_id=config.run_id,
            input_records=input_count,
            winning_records=winning_count,
            duplicate_or_superseded_records=(
                duplicate_or_superseded_count
            ),
            delete_candidates=delete_count,
            upsert_candidates=upsert_count,
        )

        LOGGER.info(
            "Executing Iceberg MERGE SQL:\n%s",
            merge_sql,
        )

        # ---------------------------------------------------------------
        # 8. Execute one atomic Iceberg MERGE snapshot commit.
        # ---------------------------------------------------------------
        spark.sql(
            merge_sql
        )

        completed_at = (
            datetime.now(
                timezone.utc
            )
        )

        duration_seconds = round(
            time.time() - started_at,
            3,
        )

        audit_values = {
            "job_name": config.job_name,
            "run_id": config.run_id,
            "history_table": config.history_table,
            "stream_table": config.stream_table,
            "target_table": config.target_table,

            "input_records": input_count,
            "winning_records": winning_count,

            "duplicate_or_superseded_records": (
                duplicate_or_superseded_count
            ),

            "delete_candidates": delete_count,
            "upsert_candidates": upsert_count,

            "status": "SUCCEEDED",
            "completed_at_utc": (
                completed_at.isoformat()
            ),
            "duration_seconds": duration_seconds,
        }

        # ---------------------------------------------------------------
        # 9. Write optional operational audit record.
        # ---------------------------------------------------------------
        write_audit_record(
            spark,
            config,
            audit_values,
        )

        log_json(
            "cdc_merge_succeeded",
            **audit_values,
        )

        winning_df.unpersist()
        combined_df.unpersist()

        job.commit()

    except Exception as exc:
        duration_seconds = round(
            time.time() - started_at,
            3,
        )

        failure_values = {
            "job_name": config.job_name,
            "run_id": config.run_id,
            "history_table": config.history_table,
            "stream_table": config.stream_table,
            "target_table": config.target_table,

            "status": "FAILED",
            "error_type": type(exc).__name__,
            "error_message": str(exc),

            "completed_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "duration_seconds": duration_seconds,
        }

        try:
            write_audit_record(
                spark,
                config,
                failure_values,
            )
        except Exception:
            LOGGER.exception(
                "Failed to write the failure audit record."
            )

        log_json(
            "cdc_merge_failed",
            **failure_values,
        )

        LOGGER.exception(
            "CDC apply job failed."
        )

        raise


if __name__ == "__main__":
    main()