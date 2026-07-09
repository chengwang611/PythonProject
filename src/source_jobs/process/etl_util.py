import math
import sys
import re
import traceback
from datetime import datetime, timedelta
from typing import Dict, Callable, Any, List, Optional

import boto3
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    when,
    col,
    upper,
    lit,
    regexp_replace,
    trim,
)
from pyspark.sql.types import StringType

from dna_common_starter_parent.common_data_access.scon_utils import (
    write_df_to_scon,
    execute_scon_query_df,
)
from dna_common_starter_parent.common_vault.read_from_vault import get_config_root
from dna_common_starter_parent.common_logging import generic_loggers

logger = generic_loggers.get_generic_logger()


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark_session_v2(
    app_name: str,
    master_url: str,
    driver_memory: str,
    executor_memory: str,
    executor_cores: str,
    driver_cores: str,
    max_number_of_executors: str,
    s3_key: str,
    s3_password: str,
    endpoint: str,
    driver_maxResultSize: str = "2g",
) -> SparkSession:
    """
    Build and configure a SparkSession with S3A + dynamic allocation settings.
    """
    logger.info(f"Initializing Spark Session for app [{app_name}]")
    logger.info(f"  Driver Memory      : {driver_memory}")
    logger.info(f"  Executor Memory    : {executor_memory}")
    logger.info(f"  Executor Cores     : {executor_cores}")
    logger.info(f"  Driver Cores       : {driver_cores}")
    logger.info(f"  Max Executors      : {max_number_of_executors}")
    logger.info(f"  Spark Endpoint     : {endpoint}")

    try:
        spark_session = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.driver.memory", driver_memory)
            .config("spark.executor.memory", executor_memory)
            .config("spark.executor.cores", executor_cores)
            .config("spark.driver.cores", driver_cores)
            .config("spark.driver.maxResultSize", driver_maxResultSize)
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "1")
            .config("spark.dynamicAllocation.maxExecutors", max_number_of_executors)
            .config("spark.sql.analyzer.maxIterations", "300")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.hadoop.fs.s3a.access.key", s3_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_password)
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .getOrCreate()
        )

        sc = spark_session.sparkContext
        hconf = sc._jsc.hadoopConfiguration()

        # S3A Hadoop configuration
        hconf.set("fs.s3a.access.key", s3_key)
        hconf.set("fs.s3a.secret.key", s3_password)
        hconf.set("fs.s3a.endpoint", endpoint)
        hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hconf.set("fs.s3a.path.style.access", "true")
        hconf.set("fs.s3a.connection.ssl.enabled", "true")
        hconf.set("fs.s3a.committer.name", "partitioned")
        hconf.set("fs.s3a.committer.staging.conflict-mode", "append")
        hconf.set("fs.s3a.committer.magic.enabled", "false")
        hconf.set("fs.s3a.connection.timeout", "300000")
        hconf.set("fs.s3a.connection.maximum", "100")
        hconf.set("fs.s3a.attempts.maximum", "20")
        hconf.set("fs.s3a.socket.recv.buffer", str(4 * 1024 * 1024))
        hconf.set("fs.s3a.socket.send.buffer", str(4 * 1024 * 1024))
        hconf.set("spark.sql.execution.arrow.enabled", "true")

        return spark_session

    except Exception as e:  # noqa: BLE001
        logger.error("Unable to initiate Spark Session")
        logger.error(traceback.format_exc())
        raise e


# ---------------------------------------------------------------------------
# Config & Vault helpers
# ---------------------------------------------------------------------------

def get_resources_base(vault_env: str) -> str:
    """
    Map vault env (GCC/SCC/NON_PROD) to a base resources folder.
    """
    if "GCC" in vault_env:
        return "/app/resources/PROD/GCC/"
    elif "SCC" in vault_env:
        return "/app/resources/PROD/SCC/"
    else:
        return "/app/resources/NON_PROD/"


def read_yaml_from_path(path: str) -> dict:
    """
    Read a YAML config from a local path.
    """
    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:  # noqa: BLE001
        logger.error(f"Unexpected error reading config from {path}: {e}")
        raise


def process_configs(
    job_name: str,
    vault_env: str,
    report_yaml_path: str,
    file_date: str,
):
    """
    Read vault config, build SparkSession, return (spark, mssql_config, yaml_config, s3_client)
    """
    vault_file = "/rbc_vault/secrets/reports_activity_pri.properties"
    config = get_config_root(vault_file)
    root = config["root"]

    master_url = root["MASTER_URL"]
    driver_memory = root["SPARK_12_DRIVER_MEMORY"]
    executor_memory = root["SPARK_12_EXECUTOR_MEMORY"]
    executor_cores = root["SPARK_M_D_I_CORES"]
    driver_cores = root["SPARK_M_E_I_CORES"]
    max_number_of_executors = root["MAX_NUMBER_OF_EXECUTORS"]
    s3_key = root["AWS_ACCESS_KEY_ID"]
    s3_password = root["AWS_SECRET_ACCESS_KEY"]
    endpoint = root["END_POINT"]

    mssql_config = {
        "connection_url": root["mssql_connection_url"],
        "username": root["mssql_username"],
        "password": root["mssql_pwd"],
        "database": root["mssql_db"],
        "batch_size": int(root.get("mssql_batch_size", "1000")),
    }

    app_name = re.sub(r"\W", "_", vault_env) + "_" + job_name + "_etl_" + file_date
    logger.info(f"App name [{app_name}]")

    spark = get_spark_session_v2(
        app_name=app_name,
        master_url=master_url,
        driver_memory=driver_memory,
        executor_memory=executor_memory,
        executor_cores=executor_cores,
        driver_cores=driver_cores,
        max_number_of_executors=max_number_of_executors,
        s3_key=s3_key,
        s3_password=s3_password,
        endpoint=endpoint,
    )

    report_yaml_full_path = (
        get_resources_base(vault_env=vault_env) + report_yaml_path
    )
    yaml_config = read_yaml_from_path(report_yaml_full_path)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_key,
        aws_secret_access_key=s3_password,
        endpoint_url=endpoint,
    )

    return spark, mssql_config, yaml_config, s3_client


# ---------------------------------------------------------------------------
# YAML from S3 + bucket name replacement
# ---------------------------------------------------------------------------

def read_yaml_from_s3(spark: SparkSession, s3_yaml_path: str) -> dict:
    """
    Read a YAML config stored as a text file on S3.
    """
    try:
        logger.info(f"Reading YAML from {s3_yaml_path}")
        df = spark.read.text(s3_yaml_path)
        yaml_str = "\n".join(row.value for row in df.collect())
        return yaml.safe_load(yaml_str)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to read YAML: {e}")
        raise


def replace_bucket_keys(input_str: str) -> str:
    """
    Replace logical bucket keys (KYC_BUCKET_NAME, etc.) with the
    real bucket names stored in the vault config.
    """
    vault_file = "/rbc_vault/secrets/reports_activity_pri.properties"
    config = get_config_root(vault_file)
    root = config["root"]

    bucket_map = {
        "KYC_BUCKET_NAME": root["KYC_BUCKET_NAME"],
        "PIM_TS_BUCKET_NAME": root["PIM_TS_BUCKET_NAME"],
        "PIM_BUCKET_NAME": root["PIM_BUCKET_NAME"],
        "PIM_TS_REFERENCE_BUCKET_NAME": root["PIM_TS_REFERENCE_BUCKET_NAME"],
        "UNITHRAX_GAM_BUCKET_NAME1": root["UNITHRAX_GAM_BUCKET_NAME1"],
        "UNITHRAX_GAM_BUCKET_NAME2": root["UNITHRAX_GAM_BUCKET_NAME2"],
    }

    for key, value in bucket_map.items():
        if key in input_str:
            input_str = input_str.replace(key, value)

    return input_str


# ---------------------------------------------------------------------------
# Load views from S3 partitions
# ---------------------------------------------------------------------------

def validate(
    df: DataFrame,
    df_name: str = "DataFrame",
    empty_check: str = "True",
) -> None:
    """
    Basic validation that DataFrame is not empty when empty_check is True.
    """
    if empty_check == "True":
        if df.rdd.isEmpty():
            raise ValueError(f"{df_name} is empty. Aborting pipeline.")


def next_day(date_str: str, path: str) -> str:
    """
    Some buckets need to use the same date, others use next day.
    """
    bucket_name_no_plus = [
        "daily_mf_client_holdings_sales",
        "fund_reference_master_fma",
        "CUCCODAXWS",  # guessed from OCR, adjust as needed
    ]

    for k in bucket_name_no_plus:
        if k in path:
            return date_str

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    next_dt = dt + timedelta(days=1)
    return next_dt.strftime("%Y-%m-%d")


def replace_empty_with_NULL_string(df: DataFrame) -> DataFrame:
    """
    For all string cols:
      - strip
      - turn literal 'NULL'/'null' (any case) into None
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            name = field.name
            df = df.withColumn(
                name,
                regexp_replace(col(name), pattern=r"(?i)\bnull\b", replacement=""),
            )
            df = df.withColumn(
                name,
                when(trim(col(name)) == "", None).otherwise(col(name)),
            )
    return df


def load_views_from_partition(
    s3_client,
    spark: SparkSession,
    s3_base_path: str,
    file_config: List[dict],
    record_date: str,
    mssql_config: Optional[dict] = None,  # kept for backwards compatibility
    upper_col_name: bool = False,
) -> None:
    """
    For each file in file_config, load from S3 and register as temp view.
    file_config element example:
    {
        "view_name": "KYC_DAILY",
        "path": "s3a://KYC_BUCKET_NAME/path",
        "format": "parquet",
        "date_partition_key": "date_partition_key",   # for parquet
        "empty_check": "True"
    }
    """
    s3_base_path = replace_bucket_keys(s3_base_path)

    for file in file_config:
        view_name = file["view_name"].upper()
        empty_check = file.get("empty_check", "True")

        try:
            logger.info(f"Started create view: {view_name}")
            fmt = file["format"].lower()

            if fmt == "parquet":
                date_partition_key = file["date_partition_key"]
                s3_path = s3_base_path + file["path"]
                record_date2 = next_day(record_date, s3_path)
                full_path = f"{s3_path}/{date_partition_key}={record_date2}"
            elif fmt == "csv":
                full_path = s3_base_path + file["path"] + ".csv"
            else:
                raise ValueError(f"Unsupported file format: {file['format']}")

            logger.info(f"Resolved full path for {view_name}: {full_path}")

            if fmt == "parquet":
                df = spark.read.parquet(full_path)
            else:
                df = (
                    spark.read.format("csv")
                    .option("header", "true")
                    .option("quote", '"')
                    .option("escape", '"')
                    .load(full_path)
                )

            validate(df, view_name, empty_check)

            if upper_col_name:
                new_cols = [c.upper() for c in df.columns]
                df = df.toDF(*new_cols)

            df = replace_empty_with_NULL_string(df)
            df.createOrReplaceTempView(view_name)
            df.printSchema()
            df.show(20, truncate=False)
            logger.info(f"Created view: {view_name} from {full_path}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"Failed to load view {view_name}: {e}")
            raise


# ---------------------------------------------------------------------------
# SQL execution helpers
# ---------------------------------------------------------------------------

def execute_sql(spark: SparkSession, view_name: str, sql_query: str) -> None:
    """
    Execute SELECT / view creation SQL and register as temp view.
    """
    allowed_prefixes = ("SELECT", "WITH", "CREATE VIEW", "CREATE OR REPLACE VIEW")
    logger.info(f"Raw SQL: {sql_query}")

    if not sql_query.strip().upper().startswith(allowed_prefixes):
        raise ValueError("Only SELECT or view creation queries are allowed.")

    try:
        logger.info(f"Executing SQL for view: {view_name}")
        spark.sql(sql_query).createOrReplaceTempView(view_name)
        spark.sql(f"SELECT * FROM {view_name}").show(10)
        logger.info(f"Executed SQL and created view: {view_name}")
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to execute SQL for view {view_name}: {e}")
        raise


# ---------------------------------------------------------------------------
# MSSQL helpers
# ---------------------------------------------------------------------------

def write_view_to_mssql(
    spark: SparkSession,
    view_name: str,
    mssql_config: dict,
    table_config: dict,
    report_date: str,
) -> None:
    """
    Write a Spark temp view into MSSQL table via write_df_to_scon.
    """
    try:
        valid_views = [t.name for t in spark.catalog.listTables()]
        if view_name not in valid_views:
            raise ValueError(f"Invalid or unregistered view name: '{view_name}'")

        table_name = table_config["table_name"]
        df = spark.table(view_name)

        # Ensure REPORT_DATE column exists and is date
        if "REPORT_DATE" in df.columns:
            df = df.drop("REPORT_DATE")
        df = df.withColumn(
            "REPORT_DATE",
            lit(report_date).cast("date"),
        )

        df = replace_empty_with_NULL_string(df)
        columns = df.columns

        # First delete existing rows for that REPORT_DATE
        query_clear = f"DELETE FROM {table_name} WHERE REPORT_DATE = '{report_date}'"
        logger.info(f"Executing clear query: {query_clear}")
        execute_scon_query_df(
            connection_url=mssql_config["connection_url"],
            username=mssql_config["username"],
            password=mssql_config["password"],
            database=mssql_config["database"],
            spark=spark,
            query=query_clear,
        )

        logger.info(
            f"Writing view {view_name} to MSSQL table {table_config['table_name']}"
        )

        write_df_to_scon(
            scon_connection_url=mssql_config["connection_url"],
            scon_connection_username=mssql_config["username"],
            scon_connection_password=mssql_config["password"],
            scon_database=mssql_config["database"],
            table=table_config["table_name"],
            df=df,
            columns=columns,
            batch_size=mssql_config.get("batch_size", 1000),
        )

        logger.info(
            f"Wrote view {view_name} to MSSQL table {table_config['table_name']}"
        )
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to write view {view_name} to MSSQL: {e}")
        raise


def load_mssql_table_as_spark_temp_table(
    spark: SparkSession,
    mssql_config: dict,
    table_name: str,
    report_date: str,
    temp_table_name: str,
) -> DataFrame:
    """
    Load from MSSQL into a Spark temp view via scon_utils query helper.
    """
    query = (
        f"SELECT * FROM {table_name} WHERE REPORT_DATE = '{report_date}'"
    )
    try:
        logger.info(f"Read view {table_name} from MSSQL start")
        logger.info(mssql_config["username"])
        logger.info(mssql_config["connection_url"])

        df = execute_scon_query_df(
            connection_url=mssql_config["connection_url"],
            username=mssql_config["username"],
            password=mssql_config["password"],
            database=mssql_config["database"],
            spark=spark,
            query=query,
        )

        df.printSchema()
        df.show(20, truncate=False)
        df.createOrReplaceTempView(temp_table_name)
        logger.info(f"Read view {table_name} from MSSQL end")
        return df
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to read view {table_name} from MSSQL: {e}")
        raise


def load_mssql_table_as_spark_temp_table_native(
    spark: SparkSession,
    mssql_config: dict,
    table_name: str,
    report_date: str,
    temp_table_name: str,
) -> DataFrame:
    """
    Load MSSQL table using Spark JDBC directly.
    """
    host_port = mssql_config["connection_url"]
    query = f"SELECT * FROM {table_name} WHERE REPORT_DATE = '{report_date}'"

    df = (
        spark.read.format("jdbc")
        .option("url", host_port)
        .option("database", mssql_config["database"])
        .option("user", mssql_config["username"])
        .option("password", mssql_config["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("query", query)
        .option("fetchsize", "5000")
        .load()
    )

    df.printSchema()
    df.show(20, truncate=False)
    df.createOrReplaceTempView(temp_table_name)
    return df


# ---------------------------------------------------------------------------
# Step driver
# ---------------------------------------------------------------------------

QueryDict = Dict[str, str]
FuncDict = Dict[str, Callable[..., Any]]


def process_steps(
    s3_client,
    spark: SparkSession,
    report_date: str,
    config: dict,
    mssql_config: dict,
    query_dict: QueryDict,
    functions: FuncDict,
    use_native_table_loads: bool = False,
) -> None:
    """
    Main step driver; executes steps from YAML config.

    Example step entry:
      - function: "load_views"
        s3_base_path: "s3a://KYC_BUCKET_NAME"
        files: [...]
      - function: "load_table"
        table: "MY_TABLE"
        temp_table: "MY_TEMP"
      - function: "execute_sql"
        view_name: "VIEW1"
        sql: "SELECT ... "
      - function: "execute_sql_by_name"
        view_name: "VIEW1"
        sql: "QUERY_KEY"
      - function: "execute_pyspark_function_by_name"
        function_name: "my_func"
    """
    try:
        for step in config["steps"]:
            fn = step["function"]

            if fn == "load_views":
                load_views_from_partition(
                    s3_client,
                    spark,
                    step["s3_base_path"],
                    step["files"],
                    report_date,
                    mssql_config=mssql_config,
                    upper_col_name=True,
                )

            elif fn == "load_table":
                table = step["table"]
                temp_table = step["temp_table"]
                if use_native_table_loads:
                    load_mssql_table_as_spark_temp_table_native(
                        spark,
                        mssql_config,
                        table,
                        report_date,
                        temp_table,
                    )
                else:
                    load_mssql_table_as_spark_temp_table(
                        spark,
                        mssql_config,
                        table,
                        report_date,
                        temp_table,
                    )

            elif fn == "execute_sql":
                execute_sql(
                    spark,
                    step["view_name"],
                    step["sql"],
                )

            elif fn == "execute_sql_by_name":
                query = query_dict[step["sql"]]
                execute_sql(
                    spark,
                    step["view_name"],
                    query,
                )

            elif fn == "execute_pyspark_function_by_name":
                function_name = step["function_name"]
                func = functions[function_name]
                func(
                    spark=spark,
                    config=config,
                    report_date=report_date,
                    mssql_config=mssql_config,
                    temp_table=step.get("temp_table"),
                )
            else:
                raise ValueError(f"Unsupported function in steps: {fn}")

    except Exception as e:  # noqa: BLE001
        logger.error(f"Error while processing steps: {e}")
        raise
