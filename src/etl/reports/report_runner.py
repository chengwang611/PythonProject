"""Report runner that executes report processors based on report name and date."""
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession
import logging

from .customer_report import CustomerReportProcessor
from .inventory_report import InventoryReportProcessor

logger = logging.getLogger(__name__)


class ReportRunner:
    """Factory class to run different report processors."""

    def __init__(self, spark: SparkSession, mssql_config: Dict[str, Any], s3_client: Optional[Any] = None):
        self.spark = spark
        self.mssql_config = mssql_config
        self.s3_client = s3_client

        # Map of report names to processor classes
        self.report_processors = {
            "customer_report": CustomerReportProcessor,
            "inventory_report": InventoryReportProcessor,
        }

    def run_report(self, report_name: str, report_date: Optional[str] = None) -> None:
        """
        Run a report processor by name.

        Args:
            report_name: Name of the report (e.g., 'customer_report', 'inventory_report')
            report_date: Date for the report in format YYYY-MM-DD (optional)

        Raises:
            ValueError: If report_name is not found
        """
        report_name = report_name.lower().strip()

        if report_name not in self.report_processors:
            available = ", ".join(self.report_processors.keys())
            raise ValueError(f"Unknown report: {report_name}. Available reports: {available}")

        logger.info(f"Starting report: {report_name} with date: {report_date}")

        # Instantiate the appropriate processor
        processor_class = self.report_processors[report_name]
        processor = processor_class(self.spark, self.mssql_config, self.s3_client)

        # Run the report with default steps
        processor.run_default()

        logger.info(f"Successfully completed report: {report_name}")

    def run_multiple_reports(self, report_names: list, report_date: Optional[str] = None) -> None:
        """
        Run multiple report processors.

        Args:
            report_names: List of report names to run
            report_date: Date for the reports in format YYYY-MM-DD (optional)
        """
        for report_name in report_names:
            try:
                self.run_report(report_name, report_date)
            except Exception as e:
                logger.error(f"Failed to run report {report_name}: {str(e)}")
                raise


def main(report_name: str, report_date: Optional[str] = None,
         spark: Optional[SparkSession] = None, mssql_config: Optional[Dict[str, Any]] = None,
         s3_client: Optional[Any] = None):
    """
    Main function to run a report processor.

    Args:
        report_name: Name of the report (e.g., 'customer_report', 'inventory_report')
        report_date: Date for the report in format YYYY-MM-DD (optional, defaults to today)
        spark: SparkSession instance (required)
        mssql_config: MSSQL configuration dict with keys: server, database, user, password (required)
        s3_client: Optional boto3 S3 client

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("ReportRunner").getOrCreate()
        >>> config = {
        ...     'server': 'localhost',
        ...     'database': 'mydb',
        ...     'user': 'user',
        ...     'password': 'pass'
        ... }
        >>> main('customer_report', '2025-12-21', spark, config)
    """
    if spark is None:
        raise ValueError("SparkSession is required")
    if mssql_config is None:
        raise ValueError("mssql_config is required")

    # Default to today if no date provided
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")

    # Validate date format
    try:
        datetime.strptime(report_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {report_date}. Expected YYYY-MM-DD")

    logger.info(f"Report Runner started - Report: {report_name}, Date: {report_date}")

    runner = ReportRunner(spark, mssql_config, s3_client)
    runner.run_report(report_name, report_date)


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("ReportRunner") \
        .master("local") \
        .getOrCreate()

    mssql_config = {
        'server': 'your-server',
        'database': 'your-db',
        'user': 'your-user',
        'password': 'your-password'
    }

    # Run customer report for today
    main("customer_report", spark=spark, mssql_config=mssql_config)

    # Run inventory report for a specific date
    main("inventory_report", "2025-12-21", spark=spark, mssql_config=mssql_config)

