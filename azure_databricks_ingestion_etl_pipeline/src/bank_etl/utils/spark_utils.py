"""Spark session utilities for Databricks pipelines."""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "DatabricksIngestionETL",
    config_overrides: Optional[dict[str, str]] = None,
) -> SparkSession:
    """Create or retrieve a SparkSession with Databricks-optimized defaults.

    When running inside Databricks, uses the existing SparkSession.
    When running locally, creates a new session.

    Args:
        app_name: Application name.
        config_overrides: Additional Spark config key-value pairs.

    Returns:
        Active SparkSession.
    """
    builder = SparkSession.builder.appName(app_name)

    # Databricks-optimized settings
    defaults = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    for k, v in defaults.items():
        builder = builder.config(k, v)

    if config_overrides:
        for k, v in config_overrides.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    logger.info("SparkSession created: %s (master=%s)", app_name, spark.sparkContext.master)
    return spark

def hello():
    """Simple hello function to test Spark session."""

    print(f"Hello from Spark! Version: {SparkSession.builder.getOrCreate().version}")

def get_or_create_spark(app_name: str = "DatabricksIngestionETL") -> SparkSession:
    """Convenience wrapper — returns the active SparkSession or creates one."""
    return get_spark_session(app_name)
