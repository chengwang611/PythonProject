# this is a etl report main file, it  takes parameters :run_date  report_name and env., it uses report_name and env  to load yaml config file from project local resource and based on report name to call difference report processor
import os
import yaml
from pyspark.sql import SparkSession
from process.sales_report import SalesReportProcessor
from process.inventory_report import InventoryReportProcessor
from process.customer_report import CustomerReportProcessor
from datetime import datetime
def load_yaml_config(report_name: str, env: str) -> dict:
    """
    Load YAML config file based on report name and environment.
    :param report_name: Name of the report
    :param env: Environment (e.g., dev, prod)
    :return: Config dictionary
    """
    config_path = os.path.join("configs", env, f"{report_name}_config.yaml")
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data
def main(run_date: str, report_name: str, env: str):
    """
    Main ETL function to process process based on report name and environment.
    :param run_date:
    :param report_name:
    :param env:
    :return:
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"{report_name}_etl_{run_date}") \
        .getOrCreate()
    # Load YAML config
    config = load_yaml_config(report_name, env)
    steps = config.get("steps", [])
    # Select and run the appropriate report processor
    if report_name == "sales_report":
        processor = SalesReportProcessor(spark, steps)
    elif report_name == "inventory_report":
        processor = InventoryReportProcessor(spark, steps)
    elif report_name == "customer_report":
        processor = CustomerReportProcessor(spark, steps)
    else:
        raise ValueError(f"Unknown report name: {report_name}")
    processor.run(run_date=run_date)
    # Stop Spark session
    spark.stop()
# Example usage
if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    report_name = "sales_report"  # or "inventory_report", "customer_report"
    env = "dev"  # or "prod"
    main(run_date, report_name, env)