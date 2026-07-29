"""Delta table writer for the silver layer with Unity Catalog integration.

Writes transformed DataFrames as Delta tables registered in Unity
Catalog under the configured catalog.schema namespace.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DeltaWriter:
    """Writes DataFrames to Delta tables managed by Unity Catalog."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "main",
        schema: str = "silver",
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

        # Ensure the catalog and schema are set as current
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {schema}")

    # ------------------------------------------------------------------
    # Table path helpers
    # ------------------------------------------------------------------
    def _full_table_name(self, table: str) -> str:
        """Return the three-level table name: catalog.schema.table."""
        return f"{self.catalog}.{self.schema}.{table}"

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------
    def write_as_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        merge_schema: bool = True,
        partition_by: Optional[str] = None,
        optimize: bool = True,
    ) -> str:
        """Write a DataFrame as a managed Delta table in Unity Catalog.

        Args:
            df: DataFrame to write.
            table_name: Target table name (unqualified).
            mode: Write mode ('overwrite', 'append', 'merge').
            merge_schema: Allow schema evolution.
            partition_by: Optional partition column.
            optimize: Run OPTIMIZE after write.

        Returns:
            Fully qualified table name.
        """
        full_name = self._full_table_name(table_name)
        logger.info("Writing Delta table: %s (mode=%s, rows=%d)", full_name, mode, df.count())

        writer = df.write.format("delta").mode(mode)

        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.saveAsTable(full_name)

        logger.info("Delta table written: %s", full_name)

        if optimize:
            self.optimize_table(table_name)

        return full_name

    def merge_into(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        update_columns: Optional[list[str]] = None,
    ) -> None:
        """Perform an UPSERT (MERGE) into an existing Delta table.

        Args:
            source_df: Source DataFrame with new/updated rows.
            target_table: Target table name (unqualified).
            merge_keys: Columns used for matching.
            update_columns: Columns to update on match (None = all non-key).
        """
        from delta.tables import DeltaTable

        full_name = self._full_table_name(target_table)
        target = DeltaTable.forName(self.spark, full_name)

        # Build merge condition
        condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_keys]
        )

        if update_columns is None:
            update_columns = [
                c for c in source_df.columns if c not in merge_keys
            ]

        update_set = {c: f"source.{c}" for c in update_columns}
        insert_values = {c: f"source.{c}" for c in source_df.columns}

        logger.info(
            "Merging into %s: keys=%s, update_cols=%s",
            full_name,
            merge_keys,
            update_columns,
        )

        target.alias("target").merge(
            source_df.alias("source"),
            condition,
        ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(
            values=insert_values
        ).execute()

        logger.info("Merge complete: %s", full_name)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    def optimize_table(self, table_name: str, zorder_by: Optional[str] = None) -> None:
        """Run OPTIMIZE on a Delta table to compact small files.

        Args:
            table_name: Unqualified table name.
            zorder_by: Optional column(s) for Z-ordering.
        """
        full_name = self._full_table_name(table_name)
        sql = f"OPTIMIZE {full_name}"
        if zorder_by:
            sql += f" ZORDER BY ({zorder_by})"

        logger.info("Optimizing: %s", sql)
        self.spark.sql(sql)
        logger.info("Optimize complete: %s", full_name)

    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> None:
        """Run VACUUM to remove old file versions.

        Args:
            table_name: Unqualified table name.
            retention_hours: Retention period (default 7 days).
        """
        full_name = self._full_table_name(table_name)
        self.spark.sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        self.spark.sql(f"VACUUM {full_name} RETAIN {retention_hours} HOURS")
        logger.info("Vacuum complete: %s", full_name)

    # ------------------------------------------------------------------
    # Read-back
    # ------------------------------------------------------------------
    def read_table(self, table_name: str) -> DataFrame:
        """Read a Delta table back as a DataFrame."""
        full_name = self._full_table_name(table_name)
        return self.spark.table(full_name)
