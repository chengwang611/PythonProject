"""I/O utility helpers for the ETL/report processors.

Provides small wrappers for reading parquet/csv/jdbc and writing to JDBC
in a consistent way so processors can call these helpers.
"""
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a parquet dataset and return a DataFrame."""
    return spark.read.parquet(path)


def read_csv(spark: SparkSession, path: str, options: Optional[Dict] = None) -> DataFrame:
    """Read a CSV and return a DataFrame. Options map to DataFrameReader.option()."""
    reader = spark.read.format("csv")
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)
    return reader.load(path)


def read_mssql_table_native(spark: SparkSession, mssql_config: Dict, table: str) -> DataFrame:
    """Load a table from MS SQL Server using Spark's JDBC reader (native).

    mssql_config expected keys: connection_url, username, password
    """
    reader = spark.read.format("jdbc")
    reader = reader.option("url", mssql_config["connection_url"]) \
                   .option("dbtable", table) \
                   .option("user", mssql_config.get("username")) \
                   .option("password", mssql_config.get("password"))
    # optional: fetchsize, driver, etc.
    if "fetchsize" in mssql_config:
        reader = reader.option("fetchsize", mssql_config["fetchsize"])
    return reader.load()


def write_df_to_mssql(df: DataFrame, mssql_config: Dict, table: str, mode: str = "append", batch_size: Optional[int] = None) -> None:
    """Write a Spark DataFrame to MS SQL Server via JDBC.

    mssql_config expected keys: connection_url, username, password, driver(optional)
    """
    writer = df.write.format("jdbc")
    writer = writer.option("url", mssql_config["connection_url"]) \
                   .option("dbtable", table) \
                   .option("user", mssql_config.get("username")) \
                   .option("password", mssql_config.get("password"))

    if "driver" in mssql_config:
        writer = writer.option("driver", mssql_config["driver"])
    if batch_size:
        writer = writer.option("batchsize", str(batch_size))

    writer.mode(mode).save()


def execute_sql_and_register(spark: SparkSession, sql: str, view_name: str) -> DataFrame:
    """Execute a SQL string on the SparkSession and register the result as a temp view."""
    df = spark.sql(sql)
    df.createOrReplaceTempView(view_name)
    return df

