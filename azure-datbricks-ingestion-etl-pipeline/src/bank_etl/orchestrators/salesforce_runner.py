"""Orchestrator: Salesforce Ingestion.

Contains all business logic for extracting Salesforce data via Bulk API 2.0
and writing it as Parquet to the raw (bronze) layer.  Shared by the notebook
and the wheel-task entry point.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from bank_etl.config import PipelineConfig, load_config
from bank_etl.ingestion.auth import OAuth2Client
from bank_etl.ingestion.salesforce_bulk import SalesforceBulkClient
from bank_etl.raw_writer.parquet_writer import ParquetWriter
from bank_etl.utils.logging_utils import setup_logging
from bank_etl.utils.spark_utils import get_or_create_spark

logger = logging.getLogger("salesforce_ingestion")


def run(
    config_path: str = "/Workspace/Shared/pipeline_config.yaml",
    trade_date: str = "",
    objects: str = "",
) -> Dict[str, Any]:
    """Execute the full Salesforce ingestion workflow.

    Parameters
    ----------
    config_path : str
        Path to ``pipeline_config.yaml`` in the workspace.
    trade_date : str
        Trade date as ``YYYY-MM-DD``.  Empty string defaults to yesterday.
    objects : str
        Comma-separated list of Salesforce object names.  Empty = all configured.

    Returns
    -------
    dict
        Summary with keys ``status``, ``trade_date``, ``objects``, ``record_counts``.
    """
    setup_logging()

    # Resolve trade_date default
    if not trade_date:
        from datetime import date, timedelta
        trade_date = (date.today() - timedelta(days=1)).isoformat()

    logger.info("=" * 60)
    logger.info("Salesforce Ingestion — trade_date=%s", trade_date)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Load configuration & Spark
    # ------------------------------------------------------------------
    config: PipelineConfig = load_config(config_path)
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
    objects_to_extract: List[str] = (
        [o.strip() for o in objects.split(",") if o.strip()]
        if objects
        else sf_cfg.objects
    )

    queries: Dict[str, str] = {}
    for obj in objects_to_extract:
        soql = (
            f"SELECT FIELDS(ALL) FROM {obj} "
            f"WHERE LastModifiedDate >= {trade_date}T00:00:00Z "
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
    results: Dict[str, List[Dict[str, Any]]] = bulk_client.run_queries(queries)

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
            trade_date=trade_date,
            mode="overwrite",
        )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("Salesforce ingestion complete for trade_date=%s", trade_date)
    logger.info("Objects processed: %s", objects_to_extract)

    return {
        "status": "success",
        "trade_date": trade_date,
        "objects": objects_to_extract,
        "record_counts": {k: len(v) for k, v in results.items()},
    }
