"""Orchestrator: ETL Pipeline (Raw → Silver).

Contains all business logic for reading raw Parquet data from both Salesforce
and PMM, validating, filtering, joining, aggregating, and writing the result as
Delta tables managed by Unity Catalog.  Shared by the notebook and the
wheel-task entry point.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from pyspark.sql import DataFrame

from bank_etl.config import PipelineConfig, load_config
from bank_etl.etl.delta_writer import DeltaWriter
from bank_etl.etl.transformer import DataTransformer
from bank_etl.etl.validator import DataValidator
from bank_etl.utils.logging_utils import setup_logging
from bank_etl.utils.spark_utils import get_or_create_spark

logger = logging.getLogger("etl_pipeline")


def run(
    config_path: str = "/Workspace/Shared/pipeline_config.yaml",
    trade_date: str = "",
) -> Dict[str, Any]:
    """Execute the full ETL pipeline (Raw → Silver).

    Parameters
    ----------
    config_path : str
        Path to ``pipeline_config.yaml`` in the workspace.
    trade_date : str
        Trade date as ``YYYY-MM-DD``.  Empty string defaults to yesterday.

    Returns
    -------
    dict
        Summary with keys ``status``, ``trade_date``, ``sf_tables_processed``,
        ``pmm_tables_processed``.
    """
    setup_logging()

    # Resolve trade_date default
    if not trade_date:
        from datetime import date, timedelta
        trade_date = (date.today() - timedelta(days=1)).isoformat()

    logger.info("=" * 60)
    logger.info("ETL Pipeline (Raw → Silver) — trade_date=%s", trade_date)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Load configuration & Spark
    # ------------------------------------------------------------------
    config: PipelineConfig = load_config(config_path)
    spark = get_or_create_spark("EtlPipeline")

    # ------------------------------------------------------------------
    # Initialize components
    # ------------------------------------------------------------------
    validator = DataValidator(
        spark,
        mode=config.etl.validation_mode,
        null_threshold_pct=config.etl.null_threshold_pct,
    )
    transformer = DataTransformer(spark)
    delta_writer = DeltaWriter(
        spark,
        catalog=config.unity_catalog.catalog,
        schema=config.unity_catalog.silver_schema,
    )

    # ------------------------------------------------------------------
    # Step 1: Read raw data
    # ------------------------------------------------------------------
    logger.info("Step 1: Reading raw Parquet data …")

    sf_tables: Dict[str, DataFrame] = {}
    for obj in config.salesforce.objects:
        obj_path = f"{config.storage.salesforce_raw_path}/{obj.lower()}/trade_date={trade_date}"
        try:
            sf_tables[obj.lower()] = spark.read.parquet(obj_path)
            logger.info("  Read salesforce/%s: %d rows", obj.lower(), sf_tables[obj.lower()].count())
        except Exception:
            logger.warning("  No data for salesforce/%s on %s", obj.lower(), trade_date)

    pmm_tables: Dict[str, DataFrame] = {}
    for ep in config.pmm.endpoints:
        table_name = ep.strip("/").replace("/", "_").replace("-", "_")
        ep_path = f"{config.storage.pmm_raw_path}/{table_name}/trade_date={trade_date}"
        try:
            pmm_tables[table_name] = spark.read.parquet(ep_path)
            logger.info("  Read pmm/%s: %d rows", table_name, pmm_tables[table_name].count())
        except Exception:
            logger.warning("  No data for pmm/%s on %s", table_name, trade_date)

    if not sf_tables and not pmm_tables:
        msg = f"No raw data found for trade_date={trade_date}. Aborting."
        logger.error(msg)
        return {"status": "skipped", "reason": msg}

    # ------------------------------------------------------------------
    # Step 2: Validate raw data
    # ------------------------------------------------------------------
    logger.info("Step 2: Validating raw data …")
    for name, df in {**sf_tables, **pmm_tables}.items():
        validator.validate_row_count(df, min_rows=1, table_name=name)
        validator.check_nulls(df, table_name=name)

    # ------------------------------------------------------------------
    # Step 3: Apply business filters
    # ------------------------------------------------------------------
    logger.info("Step 3: Applying business filters …")
    default_filters: List[str] = [
        "status != 'Deleted'",
        "is_active == true",
    ]
    for name in list(sf_tables):
        sf_tables[name] = transformer.apply_filters(
            sf_tables[name], default_filters, table_name=f"sf_{name}"
        )
    for name in list(pmm_tables):
        pmm_tables[name] = transformer.apply_filters(
            pmm_tables[name], default_filters, table_name=f"pmm_{name}"
        )

    # ------------------------------------------------------------------
    # Step 4: Write individual source tables to silver
    # ------------------------------------------------------------------
    logger.info("Step 4: Writing individual source tables to silver …")
    for name, df in sf_tables.items():
        enriched = transformer.add_audit_columns(df, source="salesforce")
        delta_writer.write_as_table(
            enriched, table_name=f"salesforce_{name}", mode="overwrite", partition_by="trade_date"
        )
    for name, df in pmm_tables.items():
        enriched = transformer.add_audit_columns(df, source="pmm")
        delta_writer.write_as_table(
            enriched, table_name=f"pmm_{name}", mode="overwrite", partition_by="trade_date"
        )

    # ------------------------------------------------------------------
    # Step 5: Join Salesforce + PMM data
    # ------------------------------------------------------------------
    logger.info("Step 5: Joining Salesforce + PMM datasets …")
    sf_primary = sf_tables.get("account", next(iter(sf_tables.values()), None) if sf_tables else None)
    pmm_primary = pmm_tables.get("v1_metrics", next(iter(pmm_tables.values()), None) if pmm_tables else None)

    if sf_primary is not None and pmm_primary is not None:
        join_keys = config.etl.join_keys
        valid_keys = [k for k in join_keys if k in sf_primary.columns and k in pmm_primary.columns]
        if valid_keys:
            joined = transformer.join_datasets(sf_primary, pmm_primary, join_keys=valid_keys, how="inner")
            joined_enriched = transformer.add_audit_columns(joined, source="joined_sf_pmm")
            delta_writer.write_as_table(
                joined_enriched, table_name="joined_salesforce_pmm", mode="overwrite", partition_by="trade_date"
            )
            logger.info("Joined dataset written: %d rows", joined.count())
        else:
            logger.warning("Join keys %s not found in both datasets. Skipping join.", join_keys)
    else:
        logger.warning(
            "Missing primary tables for join (sf=%s, pmm=%s). Skipping join.",
            sf_primary is not None,
            pmm_primary is not None,
        )

    # ------------------------------------------------------------------
    # Step 6: Aggregations → daily summary
    # ------------------------------------------------------------------
    logger.info("Step 6: Creating daily summary aggregations …")
    for name, df in {**sf_tables, **pmm_tables}.items():
        numeric_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float", "decimal")]
        if not numeric_cols:
            continue
        group_cols = ["trade_date"] if "trade_date" in df.columns else []
        if not group_cols:
            logger.warning("No trade_date column in %s — skipping aggregation.", name)
            continue
        summary = transformer.daily_summary(
            df, group_by=group_cols, metric_columns=numeric_cols[:10], table_name=f"{name}_daily"
        )
        delta_writer.write_as_table(summary, table_name=f"{name}_daily_summary", mode="overwrite")

    # ------------------------------------------------------------------
    # Step 7: Optimize all silver tables
    # ------------------------------------------------------------------
    logger.info("Step 7: Optimizing Delta tables …")
    silver_tables = spark.sql(
        f"SHOW TABLES IN {config.unity_catalog.catalog}.{config.unity_catalog.silver_schema}"
    ).collect()
    for row in silver_tables:
        delta_writer.optimize_table(row.tableName)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("ETL pipeline complete for trade_date=%s", trade_date)

    return {
        "status": "success",
        "trade_date": trade_date,
        "sf_tables_processed": list(sf_tables.keys()),
        "pmm_tables_processed": list(pmm_tables.keys()),
    }
