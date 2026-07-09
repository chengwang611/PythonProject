"""
Unit tests for CustomerReportProcessor.
"""
import os
import sys
import unittest
from pyspark.sql import SparkSession

# Ensure the project root is on sys.path so `source_jobs` package can be imported when tests
# are executed directly (unittest discovery may import modules as top-level).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.customer_report import CustomerReportProcessor

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestCustomerReportProcessor(unittest.TestCase):
    """Test CustomerReportProcessor run pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("customer_report_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Load test customer data from parquet file before each test."""
        parquet_path = os.path.join(DATA_DIR, "customers.parquet")
        df = self.spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("customers")

    def test_customer_processor_read_step(self):
        """Test CustomerReportProcessor can read from a table."""
        config = [
            {"action": "read", "table": "customers"},
        ]
        processor = CustomerReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 4)

    def test_customer_processor_drop_duplicates(self):
        """Test CustomerReportProcessor dropDuplicates step."""
        config = [
            {"action": "read", "table": "customers"},
            {"action": "dropDuplicates", "subset": ["customer_id", "name", "email"]},
        ]
        processor = CustomerReportProcessor(self.spark, config)
        result = processor.run()

        # Should have 3 unique customers (removed 1 duplicate)
        self.assertEqual(result.count(), 3)

    def test_customer_processor_with_filter_step(self):
        """Test CustomerReportProcessor filter step."""
        config = [
            {"action": "read", "table": "customers"},
            {"action": "filter", "condition": "region = 'US'"},
        ]
        processor = CustomerReportProcessor(self.spark, config)
        result = processor.run()

        # Should have 3 rows from US (including duplicate)
        self.assertEqual(result.count(), 3)

    def test_customer_processor_with_named_query(self):
        """Test CustomerReportProcessor with named SQL query."""
        class CustomerProcessorWithQueries(CustomerReportProcessor):
            queries = {
                "eu_customers": "SELECT * FROM customers WHERE region = 'EU'",
            }

        config = [
            {"action": "sql", "query": "eu_customers", "view": "european_customers"},
        ]
        processor = CustomerProcessorWithQueries(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 1)  # Only Bob from EU

        # Verify view was created
        view_result = self.spark.sql("SELECT * FROM european_customers").collect()
        self.assertEqual(len(view_result), 1)
        self.assertEqual(view_result[0]["name"], "Bob")

    def test_customer_processor_select_columns(self):
        """Test CustomerReportProcessor select step."""
        config = [
            {"action": "read", "table": "customers"},
            {"action": "select", "columns": ["customer_id", "name", "region"]},
        ]
        processor = CustomerReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(set(result.columns), {"customer_id", "name", "region"})

    def test_customer_processor_filter_and_drop_duplicates(self):
        """Test CustomerReportProcessor combining filter and dropDuplicates steps."""
        config = [
            {"action": "read", "table": "customers"},
            {"action": "filter", "condition": "region = 'US'"},
            {"action": "dropDuplicates", "subset": ["customer_id"]},
        ]
        processor = CustomerReportProcessor(self.spark, config)
        result = processor.run()

        # US region has 3 rows (including duplicate Alice), after drop duplicates: 2 unique
        self.assertEqual(result.count(), 2)


if __name__ == "__main__":
    unittest.main()
