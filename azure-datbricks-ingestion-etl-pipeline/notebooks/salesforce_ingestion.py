# Databricks parameters
dbutils.widgets.text("config_path", "/Workspace/Shared/pipeline_config.yaml", "Config file path")
dbutils.widgets.text("trade_date", "", "Trade date (YYYY-MM-DD, empty = yesterday)")
dbutils.widgets.text("objects", "", "Comma-separated SF objects (empty = all configured)")

# ---------------------------------------------------------------------------
# Salesforce Ingestion Notebook
# ---------------------------------------------------------------------------
# This notebook extracts data from Salesforce using the Bulk API 2.0 and
# writes it as Parquet to the raw (bronze) layer.
#
# Parameters:
#   config_path  — path to pipeline_config.yaml in workspace
#   trade_date   — YYYY-MM-DD (defaults to yesterday)
#   objects      — comma-separated list of SF objects to extract (optional)
# ---------------------------------------------------------------------------

import json
import logging
import sys
import os
from datetime import date, timedelta

# Add the wheel/package to the path if running from a deployed wheel
# When running as a notebook with the repo checked out, src/ is on the path.
try:
    from src.config import load_config, PipelineConfig
    from src.ingestion.auth import OAuth2Client
    from src.ingestion.salesforce_bulk import SalesforceBulkClient
    from src.raw_writer.parquet_writer import ParquetWriter
    from src.utils.spark_utils import get_or_create_spark
    from src.utils.logging_utils import setup_logging
except ImportError:
    # Fallback: assume the wheel is installed
    from ingestion.auth import OAuth2Client
    from ingestion.salesforce_bulk import SalesforceBulkClient
    from raw_writer.parquet_writer import ParquetWriter
    from utils.spark_utils import get_or_create_spark
    from utils.logging_utils import setup_logging
    from config import load_config, PipelineConfig

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup_logging()
logger = logging.getLogger("salesforce_ingestion")

# Resolve parameters
config_path = dbutils.widgets.get("config_path")
trade_date = dbutils.widgets.get("trade_date") or (date.today() - timedelta(days=1)).isoformat()
objects_filter = dbutils.widgets.get("objects")

logger.info("=" * 60)
logger.info("Salesforce Ingestion — trade_date=%s", trade_date)
logger.info("=" * 60)

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
config = load_config(config_path)
spark = get_or_create_spark("SalesforceIngestion")

# ---------------------------------------------------------------------------
# Authenticate with Salesforce
# ---------------------------------------------------------------------------
sf_cfg = config.salesforce
oauth = OAuth2Client(
    token_url=sf_cfg.auth_url,
    client_id=sf_cfg.client_id,
    client_secret=sf_cfg.client_secret,
    username=sf_cfg.username,
    password=f"{sf_cfg.password}{sf_cfg.security_token}",
)

# ---------------------------------------------------------------------------
# Build SOQL queries per object
# ---------------------------------------------------------------------------
objects_to_extract = (
    [o.strip() for o in objects_filter.split(",") if o.strip()]
    if objects_filter
    else sf_cfg.objects
)

queries = {}
for obj in objects_to_extract:
    # Build a SOQL query that filters by trade_date if the object has that field.
    # Adjust field names per your Salesforce schema.
    soql = f"SELECT FIELDS(ALL) FROM {obj} WHERE LastModifiedDate >= {trade_date}T00:00:00Z LIMIT 50000000"
    queries[obj] = soql
    logger.info("SOQL for %s: %s", obj, soql)

# ---------------------------------------------------------------------------
# Extract via Bulk API
# ---------------------------------------------------------------------------
bulk_client = SalesforceBulkClient(
    instance_url=sf_cfg.instance_url,
    oauth_client=oauth,
    api_version=sf_cfg.api_version,
)

results = bulk_client.run_queries(queries)

# ---------------------------------------------------------------------------
# Write to raw layer as Parquet
# ---------------------------------------------------------------------------
writer = ParquetWriter(
    spark=spark,
    base_path=config.storage.salesforce_raw_path,
)

for obj_name, records in results.items():
    if not records:
        logger.warning("No records returned for %s — skipping write.", obj_name)
        continue

    # Convert list of dicts to Spark DataFrame
    df = spark.createDataFrame(records)

    logger.info("Writing %s: %d rows", obj_name, df.count())

    writer.write_with_metadata(
        df=df,
        source="salesforce",
        table_name=obj_name.lower(),
        trade_date=trade_date,
        mode="overwrite",
    )

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
logger.info("Salesforce ingestion complete for trade_date=%s", trade_date)
logger.info("Objects processed: %s", objects_to_extract)

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "trade_date": trade_date,
    "objects": objects_to_extract,
    "record_counts": {k: len(v) for k, v in results.items()},
}))
