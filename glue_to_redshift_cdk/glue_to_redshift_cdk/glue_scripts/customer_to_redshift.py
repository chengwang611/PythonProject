"""
AWS Glue PySpark job:
    Lake Formation-managed Glue table -> transformation -> Amazon Redshift

Expected job arguments are injected by the CDK stack.

The example transformation is intentionally simple. Replace the SELECT list
and business rules with the approved source-to-target mapping.
"""

import sys
from typing import Dict

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_DATABASE",
    "SOURCE_TABLE",
    "REDSHIFT_CONNECTION_NAME",
    "REDSHIFT_DATABASE",
    "TARGET_SCHEMA",
    "TARGET_TABLE",
    "TempDir",
]


def parse_args() -> Dict[str, str]:
    """Read required Glue job arguments and fail early when one is missing."""
    return getResolvedOptions(sys.argv, REQUIRED_ARGS)


def transform(source_df: DataFrame) -> DataFrame:
    """
    Apply approved business transformations.

    The sample:
      * trims string fields;
      * adds an ETL processing timestamp;
      * removes exact duplicate rows.

    Replace this with explicit mappings rather than a blanket transformation
    when the source schema is contractually controlled.
    """
    result = source_df

    for field in source_df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    result = (
        result
        .dropDuplicates()
        .withColumn("etl_processed_at_utc", F.current_timestamp())
    )

    return result


def validate(df: DataFrame) -> None:
    """
    Minimal technical validation.

    In production, add business-rule checks such as:
      * primary-key null checks;
      * duplicate-key checks;
      * accepted-value checks;
      * row-count reconciliation;
      * source/target control totals.
    """
    if not df.columns:
        raise ValueError("Transformation produced a DataFrame with no columns.")

    # This executes a small Spark action and proves the plan is readable.
    # Avoid a full count on very large data unless reconciliation requires it.
    df.limit(1).collect()


def main() -> None:
    args = parse_args()

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Catalog-first access is intentional:
    #   Spark/Glue -> Glue Catalog -> Lake Formation authorization -> S3.
    #
    # Do not replace this with spark.read.parquet("s3://...") for governed
    # source data unless the architecture explicitly permits path-based access.
    source_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"],
        table_name=args["SOURCE_TABLE"],
        transformation_ctx="read_source_catalog_table",
    )

    source_df = source_dynamic_frame.toDF()
    transformed_df = transform(source_df)
    validate(transformed_df)

    target_dynamic_frame = DynamicFrame.fromDF(
        transformed_df,
        glue_context,
        "target_dynamic_frame",
    )

    target_table = (
        f'{args["TARGET_SCHEMA"]}.{args["TARGET_TABLE"]}'
    )

    # Glue's Redshift writer uses the configured JDBC connection and S3
    # temporary directory. The database user stored in Secrets Manager must
    # already have the appropriate Redshift database privileges.
    #
    # For append:
    #   GRANT USAGE ON SCHEMA <schema> ...
    #   GRANT INSERT ON TABLE <schema>.<table> ...
    #
    # For truncate/reload or MERGE, grant only the additional operations that
    # the approved load strategy actually requires.
    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=target_dynamic_frame,
        catalog_connection=args["REDSHIFT_CONNECTION_NAME"],
        connection_options={
            "database": args["REDSHIFT_DATABASE"],
            "dbtable": target_table,

            # Optional examples:
            # "preactions": (
            #     f"TRUNCATE TABLE {target_table};"
            # ),
            # "postactions": (
            #     f"ANALYZE {target_table};"
            # ),
        },
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="write_target_redshift",
    )

    job.commit()


if __name__ == "__main__":
    main()
