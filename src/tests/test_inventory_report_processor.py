"""
Unit tests for InventoryReportProcessor.
"""
import os
import sys
import unittest
from pyspark.sql import SparkSession

# Ensure the project root is on sys.path so `source_jobs` package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.inventory_report import InventoryReportProcessor

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestInventoryReportProcessor(unittest.TestCase):
    """Test InventoryReportProcessor run pipeline."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("inventory_report_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Load test inventory data from parquet file before each test."""
        parquet_path = os.path.join(DATA_DIR, "inventory.parquet")
        df = self.spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("inventory")

    def test_inventory_processor_read_step(self):
        """Test InventoryReportProcessor can read from a table."""
        config = [
            {"action": "read", "table": "inventory"},
        ]
        processor = InventoryReportProcessor(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 4)
        self.assertIn("product_name", result.columns)

    def test_inventory_processor_with_aggregate_step(self):
        """Test InventoryReportProcessor aggregate step."""
        config = [
            {"action": "read", "table": "inventory"},
            {
                "action": "aggregate",
                "groupBy": ["product_name"],
                "aggregations": {"quantity": "sum"},
            },
        ]
        processor = InventoryReportProcessor(self.spark, config)
        result = processor.run()

        # Should have 2 rows (Widget and Gadget)
        self.assertEqual(result.count(), 2)
        self.assertIn("quantity_sum", result.columns)

        # Verify aggregation values
        rows = sorted(result.collect(), key=lambda r: r["product_name"])
        gadget_row = [r for r in rows if r["product_name"] == "Gadget"][0]
        widget_row = [r for r in rows if r["product_name"] == "Widget"][0]

        self.assertEqual(gadget_row["quantity_sum"], 250)  # 50 + 200
        self.assertEqual(widget_row["quantity_sum"], 175)  # 100 + 75

    def test_inventory_processor_with_named_query(self):
        """Test InventoryReportProcessor with named SQL query."""
        class InventoryProcessorWithQueries(InventoryReportProcessor):
            queries = {
                "low_stock": "SELECT * FROM inventory WHERE quantity < 100",
            }

        config = [
            {"action": "sql", "query": "low_stock", "view": "low_inventory"},
        ]
        processor = InventoryProcessorWithQueries(self.spark, config)
        result = processor.run()

        self.assertEqual(result.count(), 2)  # Gadget (50) and Widget (75)

    def test_inventory_processor_filter_then_aggregate(self):
        """Test InventoryReportProcessor combining filter and aggregate steps."""
        config = [
            {"action": "read", "table": "inventory"},
            {"action": "filter", "condition": "warehouse = 'A'"},
            {
                "action": "aggregate",
                "groupBy": ["product_name"],
                "aggregations": {"quantity": "sum"},
            },
        ]
        processor = InventoryReportProcessor(self.spark, config)
        result = processor.run()

        # Should have 2 rows (Widget and Gadget from warehouse A)
        self.assertEqual(result.count(), 2)

        # Widget from A: 100, Gadget from A: 50
        rows = sorted(result.collect(), key=lambda r: r["product_name"])
        gadget_row = [r for r in rows if r["product_name"] == "Gadget"][0]
        widget_row = [r for r in rows if r["product_name"] == "Widget"][0]

        self.assertEqual(gadget_row["quantity_sum"], 50)
        self.assertEqual(widget_row["quantity_sum"], 100)


if __name__ == "__main__":
    unittest.main()
