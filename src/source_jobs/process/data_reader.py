from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any

class DataReader:
    """Utility class for reading data from various sources using PySpark."""

    def __init__(self, spark: SparkSession):
        """
        Initialize DataReader with a SparkSession.
        :param spark: SparkSession instance
        """
        self.spark = spark

    def read_parquet(self, path: str, **options) -> DataFrame:
        """
        Read data from a Parquet file.
        :param path: Path to the Parquet file
        :param options: Additional PySpark read options
        :return: DataFrame
        """
        return self.spark.read.parquet(path, **options)

    def read_csv(self, path: str, header: bool = True,
                 infer_schema: bool = True, **options) -> DataFrame:
        """
        Read data from a CSV file.
        :param path: Path to the CSV file
        :param header: Whether the first row is a header (default: True)
        :param infer_schema: Whether to infer schema (default: True)
        :param options: Additional PySpark read options
        :return: DataFrame
        """
        return self.spark.read.csv(path, header=header,
                                   inferSchema=infer_schema, **options)

    def read_sql_table(self, jdbc_url: str, table: str,
                       user: str, password: str, **options) -> DataFrame:
        """
        Read data from a SQL database table.
        :param jdbc_url: JDBC connection URL
        :param table: Table name
        :param user: Database user
        :param password: Database password
        :param options: Additional PySpark read options
        :return: DataFrame
        """
        return self.spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", user) \
            .option("password", password) \
            .options(**options) \
            .load()
