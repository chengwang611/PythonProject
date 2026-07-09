"""
Integration tests for multi-step processor pipelines.
"""
import unittest
from pyspark.sql import SparkSession

import sys
import os

# Add the parent directory to the path to import the source_jobs modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.sales_report import SalesReportProcessor
from etls.reports.inventory_report import InventoryReportProcessor


class TestProcessorIntegration(unittest.TestCase):
    """Integration tests for multi-step processor pipelines."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("processor_integration_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Create test data for integration tests."""
        # Create sales data with duplicate rows
        sales_data = [
            {"id": 1, "product": "Widget", "amount": 100, "category": "A"},
            {"id": 2, "product": "Gadget", "amount": 200, "category": "B"},
            {"id": 3, "product": "Widget", "amount": 150, "category": "A"},
            {"id": 1, "product": "Widget", "amount": 100, "category": "A"},  # Duplicate
        ]
        self.spark.createDataFrame(sales_data).createOrReplaceTempView("sales_data")

    def test_sales_processor_multi_step_pipeline(self):
        """Test SalesReportProcessor with a multi-step pipeline."""
        class SalesProcessorWithPipeline(SalesReportProcessor):
            queries = {
                "all_sales": "SELECT * FROM sales_data",
            }

        config = [
            {"action": "sql", "query": "all_sales", "view": "base_sales"},
            {"action": "filter", "condition": "amount >= 100"},
            {"action": "select", "columns": ["product", "amount", "category"]},
        ]
        processor = SalesProcessorWithPipeline(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 4)  # All rows have amount >= 100
        self.assertEqual(set(result.columns), {"product", "amount", "category"})

    def test_inventory_processor_multi_step_pipeline(self):
        """Test InventoryReportProcessor with multi-step pipeline including aggregate."""
        # Create inventory data
        inventory_data = [
            {"product": "A", "qty": 100, "loc": "X"},
            {"product": "B", "qty": 50, "loc": "X"},
            {"product": "A", "qty": 200, "loc": "Y"},
            {"product": "B", "qty": 75, "loc": "Y"},
        ]
        self.spark.createDataFrame(inventory_data).createOrReplaceTempView("inv_data")

        class InventoryProcessorWithPipeline(InventoryReportProcessor):
            queries = {
                "inventory": "SELECT * FROM inv_data",
            }

        config = [
            {"action": "sql", "query": "inventory"},
            {
                "action": "aggregate",
                "groupBy": ["product"],
                "aggregations": {"qty": "sum"},
            },
        ]
        processor = InventoryProcessorWithPipeline(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 2)  # 2 unique products
        rows = sorted(result.collect(), key=lambda r: r["product"])
        self.assertEqual(rows[0]["qty_sum"], 300)  # Product A: 100 + 200
        self.assertEqual(rows[1]["qty_sum"], 125)  # Product B: 50 + 75


if __name__ == "__main__":
    unittest.main()

