"""python_wheel_task entry point: Salesforce Ingestion.

Usage (via Databricks python_wheel_task)::

    package_name: databricks_ingestion_etl_pipeline
    entry_point:  entry_points.salesforce_ingestion_entry:main
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
import sys
from datetime import date, timedelta

from src.config import load_config
from src.ingestion.auth import OAuth2Client
from src.ingestion.salesforce_bulk import SalesforceBulkClient
from src.raw_writer.parquet_writer import ParquetWriter
from src.utils.logging_utils import setup_logging
from src.utils.spark_utils import get_or_create_spark

logger = logging.getLogger("salesforce_ingestion")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Salesforce Bulk API ingestion")
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
        "--objects",
        default="",
        help="Comma-separated SF objects (empty = all configured)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the Salesforce ingestion wheel task."""
    setup_logging()
    args = _parse_args(argv)

    logger.info("=" * 60)
    logger.info("Salesforce Ingestion — trade_date=%s", args.trade_date)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Load configuration & Spark
    # ------------------------------------------------------------------
    config = load_config(args.config_path)
    spark = get_or_create_spark("SalesforceIngestion")

    # ------------------------------------------------------------------
    # Authenticate with Salesforce
    # ------------------------------------------------------------------
    sf_cfg = config.salesforce
    oauth = OAuth2Client(
        token_url=sf_cfg.auth_url,
        client_id=sf_cfg.client_id,
        client_secret=sf_cfg.client_secret,
        username=sf_cfg.username,
        password=f"{sf_cfg.password}{sf_cfg.security_token}",
    )

    # ------------------------------------------------------------------
    # Build SOQL queries per object
    # ------------------------------------------------------------------
    objects_to_extract = (
        [o.strip() for o in args.objects.split(",") if o.strip()]
        if args.objects
        else sf_cfg.objects
    )

    queries = {}
    for obj in objects_to_extract:
        soql = (
            f"SELECT FIELDS(ALL) FROM {obj} "
            f"WHERE LastModifiedDate >= {args.trade_date}T00:00:00Z "
            f"LIMIT 50000000"
        )
        queries[obj] = soql
        logger.info("SOQL for %s: %s", obj, soql)

    # ------------------------------------------------------------------
    # Extract via Bulk API
    # ------------------------------------------------------------------
    bulk_client = SalesforceBulkClient(
        instance_url=sf_cfg.instance_url,
        oauth_client=oauth,
        api_version=sf_cfg.api_version,
    )
    results = bulk_client.run_queries(queries)

    # ------------------------------------------------------------------
    # Write to raw layer as Parquet
    # ------------------------------------------------------------------
    writer = ParquetWriter(spark=spark, base_path=config.storage.salesforce_raw_path)

    for obj_name, records in results.items():
        if not records:
            logger.warning("No records returned for %s — skipping write.", obj_name)
            continue

        df = spark.createDataFrame(records)
        logger.info("Writing %s: %d rows", obj_name, df.count())

        writer.write_with_metadata(
            df=df,
            source="salesforce",
            table_name=obj_name.lower(),
            trade_date=args.trade_date,
            mode="overwrite",
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("Salesforce ingestion complete for trade_date=%s", args.trade_date)
    logger.info("Objects processed: %s", objects_to_extract)

    # Databricks python_wheel_task captures stdout; print JSON for downstream
    summary = {
        "status": "success",
        "trade_date": args.trade_date,
        "objects": objects_to_extract,
        "record_counts": {k: len(v) for k, v in results.items()},
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
