"""
Base report processor with shared SQL query registry and execution helpers.
Each concrete report should subclass this and provide a `queries` dict mapping
query names to SQL strings, e.g.

class MyReportProcessor(BaseReportProcessor):
    queries = {
        "my_query": "SELECT * FROM some_table WHERE ...",
    }

During a step with action 'sql', steps should provide a `query` value which is
either a query name (looked up in `self.queries`) or a raw SQL string. The
resulting DataFrame will be registered as a temp view using `view` from the
step or the query name.
"""
from typing import Dict, Any, Optional, Callable
from pyspark.sql import SparkSession, DataFrame
import logging

from .data_reader import DataReader

logger = logging.getLogger(__name__)


class BaseReportProcessor:
    """Shared functionality for report processors.

    Supports these actions out of the box:
    - read: read from parquet/csv/sql table
    - sql: execute SQL query (named or raw)
    - filter: filter DataFrame
    - select: select columns
    - withColumn: add/modify column
    - dropDuplicates: remove duplicates
    - join: join with another DataFrame
    - aggregate: group by and aggregate
    - function: execute custom function
    - write: write to parquet/csv/sql table
    """

    # Concrete processors should override this with their named SQL queries.
    queries: Dict[str, str] = {}

    # Concrete processors can override this with custom functions
    functions: Dict[str, Callable] = {}

    def __init__(self, spark: SparkSession, config: list):
        self.spark = spark
        self.config = config
        # Provide a shared DataReader helper for read operations
        self.reader = DataReader(spark)

    def run(self, run_date: Optional[str] = None) -> Optional[DataFrame]:
        """Execute all steps in the config and return the final DataFrame.

        This is the main entry point that processes all actions defined in config.
        Child classes can override this if they need custom behavior, or simply
        use this implementation and define their queries/functions.
        """
        df = None
        for step in self.config:
            action = step.get("action")

            if action == "read":
                df = self._read(step)
            elif action == "sql":
                df = self.execute_query_step(step)
            elif action == "filter" and df is not None:
                df = df.filter(step["condition"])
            elif action == "select" and df is not None:
                df = df.select(*step["columns"])
            elif action == "withColumn" and df is not None:
                df = df.withColumn(step["name"], eval(step["expr"]))
            elif action == "dropDuplicates" and df is not None:
                subset = step.get("subset")
                df = df.dropDuplicates(subset) if subset else df.dropDuplicates()
            elif action == "join" and df is not None:
                other = self._read(step["other"]) if "other" in step else self.spark.table(step["table"])
                df = df.join(other, on=step["on"], how=step.get("how", "inner"))
            elif action == "aggregate" and df is not None:
                df = self._aggregate(df, step)
            elif action == "function":
                df = self._execute_function(df, step)
            elif action == "write" and df is not None:
                self._write(df, step, run_date)
            else:
                if action:
                    logger.warning("Unknown action '%s' in step %s", action, step)

        return df

    def _aggregate(self, df: DataFrame, step: dict) -> DataFrame:
        """Execute aggregation step.

        Step expects:
        - groupBy: list of column names to group by
        - aggregations: dict mapping column name to aggregation function (sum, count, avg, etc.)
        """
        group_by = step.get("groupBy", [])
        aggs = step.get("aggregations", {})

        if not group_by or not aggs:
            logger.warning("aggregate step missing groupBy or aggregations: %s", step)
            return df

        from pyspark.sql import functions as F
        agg_exprs = [getattr(F, func)(col).alias(f"{col}_{func}") for col, func in aggs.items()]
        return df.groupBy(*group_by).agg(*agg_exprs)

    def _execute_function(self, df: Optional[DataFrame], step: dict) -> DataFrame:
        """Execute a custom function.

        Step expects:
        - name: function name (key in self.functions dict)
        - args: optional dict of arguments to pass to the function

        The function signature should be: func(df: DataFrame, spark: SparkSession, **kwargs) -> DataFrame
        """
        func_name = step.get("name")
        if not func_name:
            raise ValueError("function step requires 'name'")

        if func_name not in self.functions:
            raise KeyError(f"Function '{func_name}' not found in processor functions")

        func = self.functions[func_name]
        args = step.get("args", {})

        return func(df, self.spark, **args)

    def execute_query_step(self, step: dict) -> DataFrame:
        """Execute a named query (or raw SQL) and register it as a temp view.

        step expects:
          - query: either a query name (key in self.queries) or a raw SQL string
          - view (optional): name to register the result as a temp view

        Returns the resulting DataFrame.
        """
        q_ref = step.get("query")
        if not q_ref:
            raise ValueError("sql step requires 'query' (name or raw SQL)")

        # Resolve query string: prefer named queries defined on the processor.
        query_str = None
        if isinstance(q_ref, str) and q_ref in getattr(self, "queries", {}):
            query_str = self.queries[q_ref]
        else:
            # If q_ref looks like raw SQL (contains whitespace or starts with SELECT), accept it
            if isinstance(q_ref, str) and (" " in q_ref or q_ref.strip().lower().startswith("select")):
                query_str = q_ref

        if not query_str:
            raise KeyError(f"Query '{q_ref}' not found in processor queries and is not valid raw SQL")

        df = self.spark.sql(query_str)

        # Register temp view: use explicit view name or fall back to the query name
        view_name = step.get("view")
        # Only register the view if an explicit view name is provided.
        # If the step used a named query (e.g., 'get_users'), allow using that name
        # when it's a valid identifier and no explicit view is given.
        if not view_name and isinstance(q_ref, str) and q_ref in getattr(self, "queries", {}):
            # Use the query key as view name only when it is a valid identifier
            if q_ref.isidentifier():
                view_name = q_ref

        if view_name:
            df.createOrReplaceTempView(view_name)

        return df

    def _read(self, step: Any) -> DataFrame:
        """Unified read helper.

        step can be either a dict describing the read step or a string table name.
        Supported dict keys: format (parquet/csv/jdbc/other), path, table, header,
        inferSchema, jdbc_url, dbtable, user, password, and any format-specific options.
        """
        # If caller passed a string, treat it as table name
        if isinstance(step, str):
            return self.spark.table(step)

        if not isinstance(step, dict):
            raise ValueError("read step must be a dict or table name string")

        fmt = step.get("format", "parquet")
        path = step.get("path")

        # Read from path if provided
        if path:
            if fmt == "parquet":
                return self.reader.read_parquet(path)
            elif fmt == "csv":
                return self.reader.read_csv(path, header=step.get("header", True), infer_schema=step.get("inferSchema", True))
            elif fmt == "jdbc":
                # For jdbc, prefer explicit jdbc_url/dbtable
                jdbc_url = step.get("jdbc_url")
                table_name = step.get("dbtable") or step.get("table")
                user = step.get("user")
                password = step.get("password")
                return self.reader.read_sql_table(jdbc_url, table_name, user, password)
            else:
                # Generic format read
                return self.spark.read.format(fmt).load(path)

        # No path: read from table name if specified
        table = step.get("table")
        if table:
            return self.spark.table(table)

        # If format is jdbc and no path but jdbc params present
        if fmt == "jdbc":
            jdbc_url = step.get("jdbc_url")
            table_name = step.get("dbtable") or step.get("table")
            user = step.get("user")
            password = step.get("password")
            if jdbc_url and table_name:
                return self.reader.read_sql_table(jdbc_url, table_name, user, password)

        raise ValueError("read step requires 'path' or 'table'")

    def _write(self, df: DataFrame, step: dict, run_date: Optional[str] = None):
        """Unified write helper using spark DataFrameWriter.
         if write to a table then the table is sql table and need to remove existing record for the same run_date before write
        step keys: format (parquet/csv/etc), path, table, mode, and any writer options.
        """

        fmt = step.get("format", "parquet")
        path = step.get("path")
        mode = step.get("mode", "overwrite")

        if path:
            if "{run_date}" in path and run_date:
                path = path.format(run_date=run_date)
            df.write.format(fmt).mode(mode).save(path)
        else:
            table = step.get("table")
            if table:
                df.write.mode(mode).saveAsTable(table)
            else:
                raise ValueError("write step requires 'path' or 'table'")
