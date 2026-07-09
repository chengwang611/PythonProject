"""Base report processor that provides shared read/query/write step implementations.

Report-specific processors should subclass BaseReportProcessor and provide:
- self.queries: dict of name->sql string
- self.steps: list of step dicts used by run()
"""
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from . import io_util
import logging

logger = logging.getLogger(__name__)


class BaseReportProcessor:
    def __init__(self, spark: SparkSession, mssql_config: Dict[str, Any], s3_client: Optional[Any] = None):
        self.spark = spark
        self.mssql_config = mssql_config
        self.s3_client = s3_client
        self.queries: Dict[str, str] = {}

    # --- Basic IO helpers delegated to io_util ---
    def _read_parquet(self, path: str) -> DataFrame:
        logger.info(f"Reading parquet from {path}")
        return io_util.read_parquet(self.spark, path)

    def _read_csv(self, path: str, options: Optional[Dict] = None) -> DataFrame:
        logger.info(f"Reading csv from {path} options={options}")
        return io_util.read_csv(self.spark, path, options)

    def _read_table_native(self, table: str) -> DataFrame:
        logger.info(f"Loading table {table} via native JDBC")
        return io_util.read_mssql_table_native(self.spark, self.mssql_config, table)

    def _write(self, df: DataFrame, table: str, mode: str = "append", batch_size: Optional[int] = None) -> None:
        logger.info(f"Writing DataFrame to table {table} mode={mode}")
        io_util.write_df_to_mssql(df, self.mssql_config, table, mode, batch_size)

    # --- Query execution ---
    def _execute_query_by_name(self, query_name: str, target_view: str) -> DataFrame:
        if query_name not in self.queries:
            raise ValueError(f"Unknown query name: {query_name}")
        sql = self.queries[query_name]
        logger.info(f"Executing named query {query_name} -> registering view {target_view}")
        return io_util.execute_sql_and_register(self.spark, sql, target_view)

    def _execute_sql(self, sql: str, target_view: str) -> DataFrame:
        logger.info(f"Executing SQL -> registering view {target_view}")
        return io_util.execute_sql_and_register(self.spark, sql, target_view)

    # --- Runner ---
    def run(self, steps: List[Dict[str, Any]], functions: Optional[Dict[str, Any]] = None) -> None:
        functions = functions or {}
        for step in steps:
            fn = step.get("function")
            if fn == "read_parquet":
                df = self._read_parquet(step["path"])
                df.createOrReplaceTempView(step["view_name"])
            elif fn == "read_csv":
                df = self._read_csv(step["path"], step.get("options"))
                df.createOrReplaceTempView(step["view_name"])
            elif fn == "read_table":
                df = self._read_table_native(step["table"])  # native by default
                df.createOrReplaceTempView(step["view_name"])
            elif fn == "execute_sql_by_name":
                self._execute_query_by_name(step["sql"], step["view_name"])  # sql contains name
            elif fn == "execute_sql":
                self._execute_sql(step["sql"], step["view_name"])
            elif fn == "execute_function":
                func = functions.get(step["function_name"]) if functions else None
                if not func:
                    raise ValueError(f"Unknown function {step.get('function_name')}")
                func(self.spark, step.get("temp_table"))
            elif fn == "write_table":
                view = step["view_name"]
                df = self.spark.table(view)
                self._write(df, step["table_name"], step.get("mode", "append"), step.get("batch_size"))
            else:
                raise ValueError(f"Unsupported step function: {fn}")

