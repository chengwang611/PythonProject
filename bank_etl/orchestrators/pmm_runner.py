"""Orchestrator: PMM Ingestion.

Contains all business logic for extracting data from the PMM REST API and
writing it as Parquet to the raw (bronze) layer.  Shared by the notebook and
the wheel-task entry point.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from bank_etl.config import PipelineConfig, load_config
from bank_etl.ingestion.pmm_api import PmmApiClient
from bank_etl.raw_writer.parquet_writer import ParquetWriter
from bank_etl.utils.logging_utils import setup_logging
from bank_etl.utils.spark_utils import get_or_create_spark

logger = logging.getLogger("pmm_ingestion")


def run(
    config_path: str = "/Workspace/Shared/pipeline_config.yaml",
    trade_date: str = "",
    endpoints: str = "",
) -> Dict[str, Any]:
    """Execute the full PMM ingestion workflow.

    Parameters
    ----------
    config_path : str
        Path to ``pipeline_config.yaml`` in the workspace.
    trade_date : str
        Trade date as ``YYYY-MM-DD``.  Empty string defaults to yesterday.
    endpoints : str
        Comma-separated list of PMM endpoint paths.  Empty = all configured.

    Returns
    -------
    dict
        Summary with keys ``status``, ``trade_date``, ``endpoints``, ``record_counts``.
    """
    setup_logging()

    # Resolve trade_date default
    if not trade_date:
        from datetime import date, timedelta
        trade_date = (date.today() - timedelta(days=1)).isoformat()

    logger.info("=" * 60)
    logger.info("PMM Ingestion — trade_date=%s", trade_date)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Load configuration & Spark
    # ------------------------------------------------------------------
    config: PipelineConfig = load_config(config_path)
    spark = get_or_create_spark("PmmIngestion")

    # ------------------------------------------------------------------
    # Initialize PMM client
    # ------------------------------------------------------------------
    pmm_cfg = config.pmm
    client = PmmApiClient(
        base_url=pmm_cfg.base_url,
        api_key=pmm_cfg.api_key,
        page_size=pmm_cfg.page_size,
        max_pages=pmm_cfg.max_pages,
    )

    # ------------------------------------------------------------------
    # Determine endpoints to extract
    # ------------------------------------------------------------------
    endpoints_list: List[str] = (
        [e.strip() for e in endpoints.split(",") if e.strip()]
        if endpoints
        else pmm_cfg.endpoints
    )
    logger.info("Endpoints to extract: %s", endpoints_list)

    # ------------------------------------------------------------------
    # Extract data from all endpoints
    # ------------------------------------------------------------------
    results: Dict[str, List[Dict[str, Any]]] = client.fetch_all_endpoints(
        endpoints=endpoints_list, trade_date=trade_date
    )

    # ------------------------------------------------------------------
    # Write to raw layer as Parquet
    # ------------------------------------------------------------------
    writer = ParquetWriter(spark=spark, base_path=config.storage.pmm_raw_path)

    for table_name, records in results.items():
        if not records:
            logger.warning("No records returned for %s — skipping write.", table_name)
            continue

        df = spark.createDataFrame(records)
        logger.info("Writing %s: %d rows", table_name, df.count())

        writer.write_with_metadata(
            df=df,
            source="pmm",
            table_name=table_name,
            trade_date=trade_date,
            mode="overwrite",
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("PMM ingestion complete for trade_date=%s", trade_date)
    logger.info("Endpoints processed: %s", endpoints_list)

    return {
        "status": "success",
        "trade_date": trade_date,
        "endpoints": endpoints_list,
        "record_counts": {k: len(v) for k, v in results.items()},
    }
