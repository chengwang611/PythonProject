# Databricks parameters
dbutils.widgets.text("config_path", "/Workspace/Shared/pipeline_config.yaml", "Config file path")
dbutils.widgets.text("trade_date", "", "Trade date (YYYY-MM-DD, empty = yesterday)")
dbutils.widgets.text("endpoints", "", "Comma-separated PMM endpoints (empty = all configured)")

# ---------------------------------------------------------------------------
# PMM Ingestion Notebook
# ---------------------------------------------------------------------------
# This notebook extracts data from the PMM REST API and writes it as
# Parquet to the raw (bronze) layer.
#
# Parameters:
#   config_path  — path to pipeline_config.yaml in workspace
#   trade_date   — YYYY-MM-DD (defaults to yesterday)
#   endpoints    — comma-separated list of PMM endpoints (optional)
# ---------------------------------------------------------------------------

import json
import logging
import sys
import os
from datetime import date, timedelta

try:
    from src.config import load_config, PipelineConfig
    from src.ingestion.pmm_api import PmmApiClient
    from src.raw_writer.parquet_writer import ParquetWriter
    from src.utils.spark_utils import get_or_create_spark
    from src.utils.logging_utils import setup_logging
except ImportError:
    from ingestion.pmm_api import PmmApiClient
    from raw_writer.parquet_writer import ParquetWriter
    from utils.spark_utils import get_or_create_spark
    from utils.logging_utils import setup_logging
    from config import load_config, PipelineConfig

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup_logging()
logger = logging.getLogger("pmm_ingestion")

config_path = dbutils.widgets.get("config_path")
trade_date = dbutils.widgets.get("trade_date") or (date.today() - timedelta(days=1)).isoformat()
endpoints_filter = dbutils.widgets.get("endpoints")

logger.info("=" * 60)
logger.info("PMM Ingestion — trade_date=%s", trade_date)
logger.info("=" * 60)

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
config = load_config(config_path)
spark = get_or_create_spark("PmmIngestion")

# ---------------------------------------------------------------------------
# Initialize PMM client
# ---------------------------------------------------------------------------
pmm_cfg = config.pmm
client = PmmApiClient(
    base_url=pmm_cfg.base_url,
    api_key=pmm_cfg.api_key,
    page_size=pmm_cfg.page_size,
    max_pages=pmm_cfg.max_pages,
)

# ---------------------------------------------------------------------------
# Determine endpoints to extract
# ---------------------------------------------------------------------------
endpoints = (
    [e.strip() for e in endpoints_filter.split(",") if e.strip()]
    if endpoints_filter
    else pmm_cfg.endpoints
)

logger.info("Endpoints to extract: %s", endpoints)

# ---------------------------------------------------------------------------
# Extract data from all endpoints
# ---------------------------------------------------------------------------
results = client.fetch_all_endpoints(endpoints=endpoints, trade_date=trade_date)

# ---------------------------------------------------------------------------
# Write to raw layer as Parquet
# ---------------------------------------------------------------------------
writer = ParquetWriter(
    spark=spark,
    base_path=config.storage.pmm_raw_path,
)

for table_name, records in results.items():
    if not records:
        logger.warning("No records returned for %s — skipping write.", table_name)
        continue

    # Convert list of dicts to Spark DataFrame
    df = spark.createDataFrame(records)

    logger.info("Writing %s: %d rows", table_name, df.count())

    writer.write_with_metadata(
        df=df,
        source="pmm",
        table_name=table_name,
        trade_date=trade_date,
        mode="overwrite",
    )

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
logger.info("PMM ingestion complete for trade_date=%s", trade_date)
logger.info("Endpoints processed: %s", endpoints)

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "trade_date": trade_date,
    "endpoints": endpoints,
    "record_counts": {k: len(v) for k, v in results.items()},
}))
