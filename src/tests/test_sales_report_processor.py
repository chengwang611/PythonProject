"""
Unit tests for SalesReportProcessor.
"""
import os
import sys
import unittest
from pyspark.sql import SparkSession

# Ensure the project root is on sys.path so `source_jobs` package can be imported when tests
# are executed directly (unittest discovery may import modules as top-level).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.sales_report import SalesReportProcessor

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestSalesReportProcessor(unittest.TestCase):
    """Test SalesReportProcessor run pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("sales_report_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Load test sales data from parquet file before each test."""
        parquet_path = os.path.join(DATA_DIR, "sales.parquet")
        df = self.spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("sales")

    def test_sales_processor_read_step(self):
        """Test SalesReportProcessor can read from a table."""
        config = [
            {"action": "read", "table": "sales"},
        ]
        processor = SalesReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 3)
        self.assertIn("product", result.columns)

    def test_sales_processor_with_filter_step(self):
        """Test SalesReportProcessor filter step."""
        config = [
            {"action": "read", "table": "sales"},
            {"action": "filter", "condition": "amount > 120"},
        ]
        processor = SalesReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 2)

    def test_sales_processor_with_select_step(self):
        """Test SalesReportProcessor select step."""
        config = [
            {"action": "read", "table": "sales"},
            {"action": "select", "columns": ["product", "amount"]},
        ]
        processor = SalesReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(set(result.columns), {"product", "amount"})

    def test_sales_processor_with_named_query(self):
        """Test SalesReportProcessor with named SQL query."""
        class SalesProcessorWithQueries(SalesReportProcessor):
            queries = {
                "high_value_sales": "SELECT * FROM sales WHERE amount > 120",
            }

        config = [
            {"action": "sql", "query": "high_value_sales", "view": "high_sales"},
        ]
        processor = SalesProcessorWithQueries(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 2)

        # Verify temp view was created
        view_result = self.spark.sql("SELECT COUNT(*) as cnt FROM high_sales").collect()
        self.assertEqual(view_result[0]["cnt"], 2)


if __name__ == "__main__":
    unittest.main()
