import os
import json
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# -------------------------------------------------------------------
# Config & logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("salesforce_ingestion")


@dataclass
class SalesforceConfig:
    instance_url: str       # e.g. "https://mydomain.my.salesforce.com"
    access_token: str       # Salesforce OAuth access token
    api_version: str = "59.0"  # adjust if needed

    @classmethod
    def from_env(cls) -> "SalesforceConfig":
        instance_url = os.environ["SALESFORCE_INSTANCE_URL"]
        access_token = os.environ["SALESFORCE_ACCESS_TOKEN"]
        api_version = os.environ.get("SALESFORCE_API_VERSION", "59.0")
        return cls(instance_url=instance_url, access_token=access_token, api_version=api_version)


@dataclass
class IngestionConfig:
    output_base_path: str   # e.g. "s3://my-bucket/salesforce/objects"
    max_retries: int = 5
    backoff_factor: int = 2  # exponential backoff: 2^attempt seconds
    ingestion_date: Optional[str] = None  # "YYYY-MM-DD"


# -------------------------------------------------------------------
# HTTP helper with retry
# -------------------------------------------------------------------

def get_with_retry(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, str]] = None,
    max_retries: int = 5,
    backoff_factor: int = 2,
    timeout: int = 30,
) -> requests.Response:
    """
    GET with simple exponential-backoff retry logic.
    Retries on non-200 responses and RequestException.
    """
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("GET %s (attempt %d)", url, attempt)
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)

            if resp.status_code == 200:
                return resp

            logger.warning(
                "Non-200 response %s: %s",
                resp.status_code,
                resp.text[:500]
            )

        except requests.RequestException as e:
            last_exception = e
            logger.warning("Request failed on attempt %d: %s", attempt, e)

        # If not returned yet, sleep and retry
        if attempt < max_retries:
            sleep_seconds = backoff_factor ** attempt
            logger.info("Sleeping %d seconds before retry...", sleep_seconds)
            time.sleep(sleep_seconds)

    # Ran out of retries
    if last_exception:
        raise RuntimeError(f"Failed to GET {url} after {max_retries} attempts") from last_exception
    else:
        raise RuntimeError(f"Failed to GET {url} after {max_retries} attempts (no exception captured)")


# -------------------------------------------------------------------
# Salesforce fetch logic (query with pagination)
# -------------------------------------------------------------------

def fetch_salesforce_object_records(
    sf_cfg: SalesforceConfig,
    object_name: str,
    max_retries: int = 5,
    backoff_factor: int = 2,
) -> List[Dict]:
    """
    Fetch all records for a Salesforce object using SOQL:
      SELECT FIELDS(ALL) FROM {object_name}

    Handles REST API pagination via nextRecordsUrl.
    """
    headers = {
        "Authorization": f"Bearer {sf_cfg.access_token}",
        "Content-Type": "application/json",
    }

    soql = f"SELECT FIELDS(ALL) FROM {object_name}"
    query_url = f"{sf_cfg.instance_url}/services/data/v{sf_cfg.api_version}/query"

    params = {"q": soql}
    all_records: List[Dict] = []

    logger.info("Starting Salesforce query for object %s", object_name)
    # First page
    response = get_with_retry(
        query_url,
        headers=headers,
        params=params,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
    )
    payload = response#.json()
    all_records.extend(payload.get("records", []))

    # Pagination via nextRecordsUrl
    while not payload.get("done", True):
        next_url_path = payload.get("nextRecordsUrl")
        if not next_url_path:
            logger.warning("Payload not done but missing nextRecordsUrl, stopping.")
            break

        next_url = f"{sf_cfg.instance_url}{next_url_path}"
        logger.info("Fetching next page: %s", next_url)

        response = get_with_retry(
            next_url,
            headers=headers,
            params=None,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        payload = response.json()
        all_records.extend(payload.get("records", []))

    logger.info("Fetched %d records for object %s", len(all_records), object_name)
    return all_records


# -------------------------------------------------------------------
# Spark / Parquet write
# -------------------------------------------------------------------

def create_spark_session(app_name: str = "SalesforceIngestion") -> SparkSession:
    """
    Create or get a SparkSession.
    Adjust configs as needed (e.g. S3 / ADLS credentials).
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def records_to_spark_df(
    spark: SparkSession,
    records: List[Dict],
    schema: Optional[StructType] = None,
):
    """
    Convert list[dict] records to Spark DataFrame.
    If schema is provided, Spark uses it; otherwise it'll infer.
    """
    if not records:
        logger.warning("No records to convert; returning empty DataFrame")
        # Empty RDD + optional schema (if provided)
        if schema is not None:
            return spark.createDataFrame([], schema=schema)
        # Fallback: empty DataFrame with a single placeholder column
        return spark.createDataFrame([], schema="dummy string")

    if schema is not None:
        return spark.createDataFrame(records, schema=schema)
    else:
        return spark.createDataFrame(records)


def write_df_as_parquet_partitioned(
    df,
    output_base_path: str,
    object_name: str,
    ingestion_date: str,
):
    """
    Write DataFrame as Parquet partitioned by ingestion_date.
    Path pattern: {output_base_path}/{object_name}/
    """
    df_with_partition = df.withColumn(
        "ingestion_date", F.lit(ingestion_date)
    )

    output_path = os.path.join(output_base_path, object_name)

    logger.info(
        "Writing DataFrame to %s partitioned by ingestion_date=%s",
        output_path,
        ingestion_date,
    )

    (
        df_with_partition
        .write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )
# write me a functionthe can read the confiig from a yaml file instead of environment variables
import yaml

def load_salesforce_config_from_yaml(file_path: str) -> SalesforceConfig:
    """
    Load SalesforceConfig from a YAML file.
    The YAML file should contain:
      instance_url: str
      access_token: str
      api_version: str (optional)
    """
    with open(file_path, 'r') as f:
        config_data = yaml.safe_load(f)

    instance_url = config_data["instance_url"]
    access_token = config_data["access_token"]
    api_version = config_data.get("api_version", "59.0")

    return SalesforceConfig(
        instance_url=instance_url,
        access_token=access_token,
        api_version=api_version
    )

# -------------------------------------------------------------------
# Main pipeline function
# -------------------------------------------------------------------

def run_salesforce_ingestion(
    object_name: str,
    sf_cfg: Optional[SalesforceConfig] = None,
    ing_cfg: Optional[IngestionConfig] = None,
):
    """
    Full pipeline:
      1. Load configs (Salesforce + ingestion).
      2. Fetch records from Salesforce (with retry).
      3. Create Spark DataFrame.
      4. Write as partitioned Parquet.
    """
    if sf_cfg is None:
        sf_cfg = SalesforceConfig.from_env()

    if ing_cfg is None:
        ingestion_date = date.today().isoformat()
        output_base_path = os.environ["OUTPUT_BASE_PATH"]  # e.g. s3://bucket/salesforce
        ing_cfg = IngestionConfig(
            output_base_path=output_base_path,
            ingestion_date=ingestion_date,
        )

    if not ing_cfg.ingestion_date:
        ing_cfg.ingestion_date = date.today().isoformat()

    # 1. Fetch records
    records = fetch_salesforce_object_records(
        sf_cfg=sf_cfg,
        object_name=object_name,
        max_retries=ing_cfg.max_retries,
        backoff_factor=ing_cfg.backoff_factor,
    )

    # 2. Spark DataFrame
    spark = create_spark_session()
    df = records_to_spark_df(spark, records)

    # 3. Write to Parquet
    write_df_as_parquet_partitioned(
        df=df,
        output_base_path=ing_cfg.output_base_path,
        object_name=object_name,
        ingestion_date=ing_cfg.ingestion_date,
    )

    logger.info(
        "Ingestion finished for object=%s, records=%d, date=%s",
        object_name,
        len(records),
        ing_cfg.ingestion_date,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Salesforce ingestion to Parquet")
    parser.add_argument(
        "--object-name",
        required=True,
        help="Salesforce object name, e.g. Account, Contact, Opportunity"
    )
    args = parser.parse_args()

    run_salesforce_ingestion(object_name=args.object_name)
