"""ETL transformation module.

Handles filtering, joining, and aggregation of raw data from
Salesforce and PMM sources to produce silver-layer datasets.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Applies business transformations to raw data."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------
    def apply_filters(
        self,
        df: DataFrame,
        filters: List[str],
        table_name: str = "unknown",
    ) -> DataFrame:
        """Apply a list of SQL-style filter expressions.

        Args:
            df: Input DataFrame.
            filters: List of filter expressions, e.g. ['status != "Deleted"', 'amount > 0'].
            table_name: For logging.

        Returns:
            Filtered DataFrame.
        """
        before = df.count()
        result = df
        for f in filters:
            result = result.filter(f)
        after = result.count()
        logger.info(
            "[%s] Filters applied: %d → %d rows (removed %d)",
            table_name,
            before,
            after,
            before - after,
        )
        return result

    # ------------------------------------------------------------------
    # Joining
    # ------------------------------------------------------------------
    def join_datasets(
        self,
        left: DataFrame,
        right: DataFrame,
        join_keys: List[str],
        how: str = "inner",
        left_alias: str = "sf",
        right_alias: str = "pmm",
    ) -> DataFrame:
        """Join two DataFrames on common keys.

        Args:
            left: Left DataFrame (e.g., Salesforce).
            right: Right DataFrame (e.g., PMM).
            join_keys: Columns to join on.
            how: Join type ('inner', 'left', 'right', 'full_outer').
            left_alias: Alias prefix for left columns.
            right_alias: Alias prefix for right columns.

        Returns:
            Joined DataFrame.
        """
        left_count = left.count()
        right_count = right.count()

        # Build join condition
        condition = None
        for key in join_keys:
            eq = left[key] == right[key]
            condition = eq if condition is None else condition & eq

        if condition is None:
            raise ValueError("At least one join key is required.")

        joined = left.alias(left_alias).join(
            right.alias(right_alias),
            on=condition,
            how=how,
        )

        result_count = joined.count()
        logger.info(
            "Join (%s): left=%d, right=%d, result=%d, keys=%s, how=%s",
            how,
            left_count,
            right_count,
            result_count,
            join_keys,
            how,
        )
        return joined

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------
    def aggregate(
        self,
        df: DataFrame,
        group_by: List[str],
        aggregations: Dict[str, str],
        table_name: str = "unknown",
    ) -> DataFrame:
        """Apply aggregations grouped by specified columns.

        Args:
            df: Input DataFrame.
            group_by: Columns to group by.
            aggregations: Dict mapping output_column → aggregation_expression.
                          e.g. {'total_amount': 'sum(amount)', 'cnt': 'count(*)'}.
            table_name: For logging.

        Returns:
            Aggregated DataFrame.
        """
        agg_exprs = []
        for out_col, expr in aggregations.items():
            agg_exprs.append(when(lit(True), lit(True)).alias("_dummy"))  # placeholder
            # Build using spark SQL expression
            from pyspark.sql.functions import expr as spark_expr

        # Rebuild properly
        agg_exprs_clean = [spark_expr(f"{v} as {k}") for k, v in aggregations.items()]

        result = df.groupBy(*group_by).agg(*agg_exprs_clean)

        logger.info(
            "[%s] Aggregation: %d groups, metrics=%s",
            table_name,
            result.count(),
            list(aggregations.keys()),
        )
        return result

    # ------------------------------------------------------------------
    # Common aggregations
    # ------------------------------------------------------------------
    def daily_summary(
        self,
        df: DataFrame,
        group_by: List[str],
        metric_columns: List[str],
        table_name: str = "daily_summary",
    ) -> DataFrame:
        """Generate a standard daily summary with count, sum, min, max.

        Args:
            df: Input DataFrame.
            group_by: Grouping columns (must include trade_date).
            metric_columns: Numeric columns to summarize.
            table_name: Output table name.

        Returns:
            Aggregated DataFrame with _count, _sum_<col>, _min_<col>, _max_<col>.
        """
        aggs: Dict[str, str] = {"row_count": "count(*)"}
        for mc in metric_columns:
            aggs[f"sum_{mc}"] = f"sum({mc})"
            aggs[f"min_{mc}"] = f"min({mc})"
            aggs[f"max_{mc}"] = f"max({mc})"
            aggs[f"avg_{mc}"] = f"avg({mc})"

        return self.aggregate(df, group_by, aggs, table_name)

    # ------------------------------------------------------------------
    # Column helpers
    # ------------------------------------------------------------------
    @staticmethod
    def add_audit_columns(df: DataFrame, source: str = "") -> DataFrame:
        """Add standard audit columns: _processed_ts, _pipeline_source."""
        return df.withColumn("_processed_ts", current_timestamp()).withColumn(
            "_pipeline_source", lit(source)
        )
