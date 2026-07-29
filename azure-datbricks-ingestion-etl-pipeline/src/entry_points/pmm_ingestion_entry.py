"""python_wheel_task entry point: PMM Ingestion.

Usage (via Databricks python_wheel_task)::

    package_name: databricks_ingestion_etl_pipeline
    entry_point:  entry_points.pmm_ingestion_entry:main
    parameters:
      - "--config_path"
      - "/Workspace/Shared/pipeline_config.yaml"
      - "--trade_date"
      - "{{job.parameter.trade_date}}"
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta

from src.config import load_config
from src.ingestion.pmm_api import PmmApiClient
from src.raw_writer.parquet_writer import ParquetWriter
from src.utils.logging_utils import setup_logging
from src.utils.spark_utils import get_or_create_spark

logger = logging.getLogger("pmm_ingestion")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PMM REST API ingestion")
    parser.add_argument(
        "--config_path",
        default="/Workspace/Shared/pipeline_config.yaml",
        help="Path to pipeline_config.yaml in workspace",
    )
    parser.add_argument(
        "--trade_date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Trade date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--endpoints",
        default="",
        help="Comma-separated PMM endpoints (empty = all configured)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the PMM ingestion wheel task."""
    setup_logging()
    args = _parse_args(argv)

    logger.info("=" * 60)
    logger.info("PMM Ingestion — trade_date=%s", args.trade_date)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Load configuration & Spark
    # ------------------------------------------------------------------
    config = load_config(args.config_path)
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
    endpoints = (
        [e.strip() for e in args.endpoints.split(",") if e.strip()]
        if args.endpoints
        else pmm_cfg.endpoints
    )
    logger.info("Endpoints to extract: %s", endpoints)

    # ------------------------------------------------------------------
    # Extract data from all endpoints
    # ------------------------------------------------------------------
    results = client.fetch_all_endpoints(endpoints=endpoints, trade_date=args.trade_date)

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
            trade_date=args.trade_date,
            mode="overwrite",
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("PMM ingestion complete for trade_date=%s", args.trade_date)
    logger.info("Endpoints processed: %s", endpoints)

    summary = {
        "status": "success",
        "trade_date": args.trade_date,
        "endpoints": endpoints,
        "record_counts": {k: len(v) for k, v in results.items()},
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
