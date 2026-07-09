# process/customer_report.py

from typing import Dict

from .base_report import BaseReportProcessor


class CustomerReportProcessor(BaseReportProcessor):
    """
    Processor that executes a list of steps (loaded from YAML) to produce a customer report.

    Supports all actions from BaseReportProcessor:
    - read: read from parquet/csv/sql table
    - sql: execute SQL query (named or raw)
    - filter, select, withColumn, dropDuplicates, join, aggregate
    - function: execute custom function
    - write: write to parquet/csv/sql table

    Define custom queries or functions as class attributes:
    queries = {"query_name": "SELECT ..."}
    functions = {"func_name": lambda df, spark, **kwargs: ...}
    """

    # Define customer-specific SQL queries here
    queries: Dict[str, str] = {}

    # Define customer-specific functions here
    functions: Dict[str, callable] = {}

    # run() is inherited from BaseReportProcessor

