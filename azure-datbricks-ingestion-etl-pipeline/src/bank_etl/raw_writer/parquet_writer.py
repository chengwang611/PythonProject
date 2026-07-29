"""Parquet writer for the raw (bronze) layer.

Writes Spark DataFrames as Parquet files to ADLS Gen2 or DBFS,
partitioned by trade_date.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class ParquetWriter:
    """Writes DataFrames as partitioned Parquet to the raw layer."""

    def __init__(
        self,
        spark: SparkSession,
        base_path: str,
        partition_column: str = "trade_date",
    ) -> None:
        self.spark = spark
        self.base_path = base_path.rstrip("/")
        self.partition_column = partition_column

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def write(
        self,
        df: DataFrame,
        source: str,
        table_name: str,
        trade_date: str,
        mode: str = "overwrite",
    ) -> str:
        """Write a DataFrame as Parquet to the raw layer.

        Args:
            df: Spark DataFrame to write.
            source: Source system name ('salesforce' or 'pmm').
            table_name: Logical table/object name.
            trade_date: Partition value (YYYY-MM-DD).
            mode: Write mode ('overwrite', 'append', etc.).

        Returns:
            The output path written to.
        """
        output_path = f"{self.base_path}/{source}/{table_name}/{self.partition_column}={trade_date}"

        logger.info(
            "Writing %d rows to %s (mode=%s)",
            df.count(),
            output_path,
            mode,
        )

        df.write.mode(mode).parquet(output_path)
        logger.info("Write complete: %s", output_path)
        return output_path

    def write_with_metadata(
        self,
        df: DataFrame,
        source: str,
        table_name: str,
        trade_date: str,
        mode: str = "overwrite",
    ) -> str:
        """Write with added metadata columns (_ingestion_ts, _source).

        This is the recommended method for raw layer writes.
        """
        from pyspark.sql.functions import current_timestamp, lit

        enriched = (
            df.withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source", lit(source))
            .withColumn("_trade_date", lit(trade_date))
        )

        return self.write(enriched, source, table_name, trade_date, mode)

    # ------------------------------------------------------------------
    # Read-back (for verification)
    # ------------------------------------------------------------------
    def read(
        self,
        source: str,
        table_name: str,
        trade_date: Optional[str] = None,
    ) -> DataFrame:
        """Read raw Parquet back into a DataFrame."""
        if trade_date:
            path = f"{self.base_path}/{source}/{table_name}/{self.partition_column}={trade_date}"
        else:
            path = f"{self.base_path}/{source}/{table_name}"

        logger.info("Reading raw parquet from %s", path)
        return self.spark.read.parquet(path)
