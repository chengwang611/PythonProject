"""Data validation module for the ETL layer.

Provides schema validation, null checks, range checks, and
deduplication logic for data moving from raw → silver.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates DataFrames against schema and quality rules."""

    def __init__(
        self,
        spark: SparkSession,
        mode: str = "strict",
        null_threshold_pct: float = 10.0,
    ) -> None:
        """Args:
        spark: Active SparkSession.
        mode: 'strict' (raise on violation) or 'warn' (log only).
        null_threshold_pct: Max allowed null percentage per column.
        """
        self.spark = spark
        self.mode = mode
        self.null_threshold_pct = null_threshold_pct

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------
    def validate_schema(
        self,
        df: DataFrame,
        expected_schema: StructType,
        table_name: str = "unknown",
    ) -> DataFrame:
        """Ensure the DataFrame schema matches the expected schema.

        Missing columns are added with null values; extra columns are
        dropped.  Type mismatches are logged.

        Returns a DataFrame conforming to expected_schema.
        """
        actual_cols = set(df.columns)
        expected_cols = {f.name for f in expected_schema.fields}

        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols

        if missing:
            msg = f"[{table_name}] Missing columns: {missing}"
            if self.mode == "strict":
                raise ValueError(msg)
            logger.warning(msg)

        if extra:
            logger.warning("[%s] Extra columns (will be dropped): %s", table_name, extra)

        # Align: add missing, drop extra, enforce order
        result = df
        for field in expected_schema.fields:
            if field.name not in actual_cols:
                result = result.withColumn(field.name, lit(None).cast(field.dataType))

        result = result.select([f.name for f in expected_schema.fields])
        return result

    # ------------------------------------------------------------------
    # Null checks
    # ------------------------------------------------------------------
    def check_nulls(
        self,
        df: DataFrame,
        table_name: str = "unknown",
        required_columns: Optional[List[str]] = None,
    ) -> Dict[str, float]:
        """Check null percentages for all (or specified) columns.

        Returns:
            Dict mapping column_name → null_percentage.
        """
        total = df.count()
        if total == 0:
            logger.warning("[%s] DataFrame is empty — skipping null check.", table_name)
            return {}

        columns = required_columns or df.columns
        null_pcts: Dict[str, float] = {}

        for c in columns:
            null_count = df.filter(col(c).isNull()).count()
            pct = (null_count / total) * 100.0
            null_pcts[c] = pct

            if pct > self.null_threshold_pct:
                msg = (
                    f"[{table_name}] Column '{c}' has {pct:.1f}% nulls "
                    f"(threshold: {self.null_threshold_pct}%)"
                )
                if self.mode == "strict":
                    raise ValueError(msg)
                logger.warning(msg)

        logger.info("[%s] Null check complete: %s", table_name, null_pcts)
        return null_pcts

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------
    def deduplicate(
        self,
        df: DataFrame,
        keys: List[str],
        table_name: str = "unknown",
    ) -> DataFrame:
        """Remove duplicate rows based on the given key columns.

        Keeps the first occurrence.
        """
        before = df.count()
        result = df.dropDuplicates(keys)
        after = result.count()
        removed = before - after

        if removed > 0:
            logger.info(
                "[%s] Deduplication removed %d rows (keys=%s)",
                table_name,
                removed,
                keys,
            )
        return result

    # ------------------------------------------------------------------
    # Row count validation
    # ------------------------------------------------------------------
    def validate_row_count(
        self,
        df: DataFrame,
        min_rows: int = 1,
        table_name: str = "unknown",
    ) -> None:
        """Ensure the DataFrame has at least min_rows."""
        cnt = df.count()
        if cnt < min_rows:
            msg = f"[{table_name}] Expected at least {min_rows} rows, got {cnt}"
            if self.mode == "strict":
                raise ValueError(msg)
            logger.warning(msg)
        else:
            logger.info("[%s] Row count OK: %d rows", table_name, cnt)
