"""
PHN PMM ingestion: call REST API, save JSON to S3 temp, load with Spark, write parquet.
"""

import os
import sys
import json
import logging
import traceback
from typing import List, Tuple

from datetime import datetime  # noqa: F401  # kept in case you need later

import boto3
from botocore.exceptions import ClientError
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


# -----------------------------------------------------------------------------
# Logging / env
# -----------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# honour proxy env (safe defaults if not set)
os.environ.setdefault("no_proxy", ".rbc.com,.devfg.rbc.com,.devfg.rbc.ca")
os.environ.setdefault("NO_PROXY", os.environ["no_proxy"])


# -----------------------------------------------------------------------------
# OAuth + REST helpers
# -----------------------------------------------------------------------------


def get_oauth_token(client_id: str, client_secret: str, login_url: str) -> Tuple[str, str]:
    """
    Do client_credentials OAuth flow.

    Returns:
        (access_token, instance_url)
    """
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(login_url, data=payload, verify=False)

    if response.status_code == 200:
        body = response.json()
        token = body.get("access_token")
        instance_url = body.get("instance_url")
        return token, instance_url
    else:
        raise Exception(f"Failed to get token: {response.text}")


# -----------------------------------------------------------------------------
# JSON <-> Parquet helpers
# -----------------------------------------------------------------------------


def load_and_save_json_as_parquet(
    spark: SparkSession, json_path: str, parquet_output_path: str
):
    """
    Read JSON file (with top-level 'items' array), flatten and write parquet.

    JSON expected:
      { "items": [ { "productId": "...", "attributeId": "...", "value": "...", "series": "..." }, ... ] }
    """
    df = spark.read.json(json_path)

    # Flatten items array
    df = df.select(explode("items").alias("items")).select("items.*")

    # Select main columns (adjust if schema changes)
    df = df.select("productId", "attributeId", "value", "series")

    df.write.mode("overwrite").parquet(parquet_output_path)

    logger.info("JSON loaded from %s and saved to %s", json_path, parquet_output_path)
    df.printSchema()
    df.show(20, truncate=False)
    return df


# -----------------------------------------------------------------------------
# S3 helpers
# -----------------------------------------------------------------------------


def delete_s3_compatible_folder(
    bucket_name: str,
    prefix: str,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    """
    Delete all objects under prefix in an S3-compatible bucket.
    """
    try:
        s3 = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        bucket = s3.Bucket(bucket_name)
        objects_to_delete = bucket.objects.filter(Prefix=prefix)

        count = 0
        for obj in objects_to_delete:
            obj.delete()
            count += 1

        logger.info("Deleted %s objects under s3://%s/%s", count, bucket_name, prefix)

    except ClientError as e:
        logger.error("Failed to delete objects from s3://%s/%s: %s", bucket_name, prefix, e)


# -----------------------------------------------------------------------------
# Attribute API helpers
# -----------------------------------------------------------------------------


def get_attribute_id(token: str, attribute_id_url: str) -> List[str]:
    """
    Call attribute ID endpoint, return list of ids as strings.
    """
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(attribute_id_url, headers=headers, verify=False)

    if response.status_code == 200:
        items = response.json()
        all_ids = [str(item["id"]) for item in items if "id" in item]
        return all_ids
    else:
        raise Exception(f"Failed to retrieve attribute ids: {response.text}")


def get_attribute_value(token: str, attribute_value_url: str):
    """
    Call attribute value endpoint, return list of attributeValues.
    """
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(attribute_value_url, headers=headers, verify=False)

    if response.status_code == 200:
        result = response.json()
        items = result["attributeValues"]
        return items
    else:
        raise Exception(f"Failed to retrieve attribute values: {response.text}")


def save_json_on_temp_folder(
    data,
    s3_bucket: str,
    s3_temp_folder: str,
    s3_client_id: str,
    s3_client_secret: str,
    s3_endpoint: str,
) -> int:
    """
    Save attribute values JSON array under a temp folder in S3.

    Layout:
      s3://bucket/{s3_temp_folder}/attributes.json

    Returns:
        total record count
    """
    # Clean up old content first
    delete_s3_compatible_folder(
        bucket_name=s3_bucket,
        prefix=s3_temp_folder,
        endpoint_url=s3_endpoint,
        access_key=s3_client_id,
        secret_key=s3_client_secret,
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_client_id,
        aws_secret_access_key=s3_client_secret,
    )

    s3_key = f"{s3_temp_folder}/attributes.json"
    logger.info("Writing temp JSON to s3://%s/%s", s3_bucket, s3_key)

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps({"items": data}),
        ContentType="application/json",
    )

    total_record_count = len(data)
    return total_record_count


# -----------------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------------


def run(
    spark: SparkSession,
    client_id: str,
    client_secret: str,
    login_url: str,
    object_name: str,
    s3_client_id: str,
    s3_client_secret: str,
    s3_endpoint: str,
    s3_bucket: str,
    s3_temp_folder: str,
    parquet_output_path: str,
    attribute_id_url: str,
    attribute_value_url: str,
) -> None:
    """
    Orchestrate PMM ingestion for a single object:
      1. OAuth
      2. Retrieve attribute IDs
      3. Retrieve attribute values
      4. Save JSON to temp S3 folder
      5. Load JSON as Spark DF & write Parquet
    """

    # Step 1: OAuth
    token, instance_url = get_oauth_token(client_id, client_secret, login_url)
    logger.info("Obtained token for instance %s", instance_url)

    # If URLs need formatting with instance_url/object_name, do that here:
    attr_id_url_full = attribute_id_url
    attr_val_url_full = attribute_value_url

    # Step 2: attribute IDs (currently not used further, but keep call for completeness)
    _all_ids = get_attribute_id(token, attr_id_url_full)

    # Step 3: attribute values
    attribute_values = get_attribute_value(token, attr_val_url_full)

    # Step 4: save JSON on temp folder
    total_record_count = save_json_on_temp_folder(
        attribute_values,
        s3_bucket,
        s3_temp_folder,
        s3_client_id,
        s3_client_secret,
        s3_endpoint,
    )

    # Step 5: read JSON & write parquet
    if total_record_count > 0:
        json_path = f"s3a://{s3_bucket}/{s3_temp_folder}/attributes.json"
        df = load_and_save_json_as_parquet(spark, json_path, parquet_output_path)
        logger.info(
            "%s with total rows %s saved to %s",
            object_name,
            df.count(),
            parquet_output_path,
        )
    else:
        logger.info("No data returned from the query for %s.", object_name)
        raise Exception(f"No data returned from the query for {object_name}.")


# -----------------------------------------------------------------------------
# CLI entry
# -----------------------------------------------------------------------------


def main() -> None:
    """
    CLI entrypoint. Reads vault config, creates Spark session, loops over object_names
    and calls run() for each.

    NOTE: RBC internal modules are imported lazily here so unit tests can import
    this file without having those wheels installed locally.
    """
    app_name = ""
    spark = None

    # Local imports for RBC internal libs (so unit tests don't break)
    from dna_common_starter_parent.common_vault_util import read_from_vault
    from dna_common_starter_parent.common_spark import spark_config

    try:
        job_name = sys.argv[1]
        file_date = sys.argv[2]      # yyyy-MM-dd
        object_names = sys.argv[3]   # comma-separated list
        vault_env = sys.argv[4]

        vault_file = "/rbc_vault/secrets/source_phn_pmm.properties"
        root = read_from_vault.get_config_root(vault_file)["root"]

        # Common Spark + S3 config
        master_url = root["MASTER_URL"]
        driver_memory = root["SPARK_I2_DRIVER_MEMORY"]
        executor_memory = root["SPARK_I2_EXECUTOR_MEMORY"]
        executor_cores = root["SPARK_M_D_I_CORES"]
        driver_cores = root["SPARK_M_D_I_CORES"]
        max_number_of_executors = root["MAX_NUMBER_OF_EXECUTORS"]

        s3_key = root["AWS_ACCESS_KEY_ID"]
        s3_password = root["AWS_SECRET_ACCESS_KEY"]
        s3_endpoint = root["END_POINT"]

        # PHN PMM specific
        client_id = root["PHN_PMM_CLIENT_ID"]
        client_secret = root["PHN_PMM_CLIENT_SECRET"]
        login_url = root["PHN_PMM_LOGIN_URL"]
        s3_bucket = root["PHN_PMM_S3_BUCKET"]
        attribute_id_url = root["PHN_PMM_ATTRIBUTE_URL"]
        attribute_value_url = root["PHN_PMM_ATTRIBUTE_VALUES_URL"]

        app_name = f"source_phn_pmm_ingestion_{vault_env}_{file_date}"
        logger.info("Starting app: %s", app_name)

        spark = spark_config.get_spark_session(
            app_name,
            master_url,
            driver_memory,
            executor_memory,
            executor_cores,
            driver_cores,
            max_number_of_executors,
            s3_key,
            s3_password,
            s3_endpoint,
        )

        logger.info("job_name    : %s", job_name)
        logger.info("file_date   : %s", file_date)
        logger.info("object_names: %s", object_names)
        logger.info("vault_env   : %s", vault_env)

        object_list = [item.strip() for item in object_names.split(",")]

        parquet_output_base = (
            f"s3a://{s3_bucket}/data/processed/phn_pmm/process_date={file_date}"
        )

        for object_name in object_list:
            s3_temp_folder = f"data/temp/phn_pmm/{object_name}/{file_date}"
            parquet_output_path = f"{parquet_output_base}/{object_name}"

            run(
                spark,
                client_id,
                client_secret,
                login_url,
                object_name,
                s3_key,
                s3_password,
                s3_endpoint,
                s3_bucket,
                s3_temp_folder,
                parquet_output_path,
                attribute_id_url,
                attribute_value_url,
            )

    except Exception as e:
        logger.error("❌ %s failed: %s", app_name, e)
        traceback.print_exc()
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
