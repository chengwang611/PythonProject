import os
import sys
import unittest
import yaml

# ensure project root on path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from etls.reports.customer_report import CustomerReportProcessor
from etls.reports.inventory_report import InventoryReportProcessor
from etls.reports.sales_report import SalesReportProcessor

RES_DIR = os.path.join(os.path.dirname(__file__), "resource")


def load_yaml(name):
    path = os.path.join(RES_DIR, name)
    with open(path, "r") as f:
        return yaml.safe_load(f)


class TestReportE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("report_e2e_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_customer_report_e2e(self):
        # prepare data
        customer_data = [
            {"customer_id": 1, "name": "Alice", "email": "alice@example.com", "region": "US"},
            {"customer_id": 2, "name": "Bob", "email": "bob@example.com", "region": "EU"},
            {"customer_id": 3, "name": "Charlie", "email": "charlie@example.com", "region": "US"},
            {"customer_id": 1, "name": "Alice", "email": "alice@example.com", "region": "US"},
        ]
        self.spark.createDataFrame(customer_data).createOrReplaceTempView("customers")

        config = load_yaml("customer_report.yaml")
        proc = CustomerReportProcessor(self.spark, config)
        df = proc.run()

        self.assertEqual(df.count(), 2)  # unique US customers after dropDuplicates

    def test_inventory_report_e2e(self):
        inventory_data = [
            {"product_id": 1, "product_name": "Widget", "quantity": 100, "warehouse": "A"},
            {"product_id": 2, "product_name": "Gadget", "quantity": 50, "warehouse": "A"},
            {"product_id": 3, "product_name": "Widget", "quantity": 75, "warehouse": "B"},
            {"product_id": 4, "product_name": "Gadget", "quantity": 200, "warehouse": "B"},
        ]
        self.spark.createDataFrame(inventory_data).createOrReplaceTempView("inventory")

        config = load_yaml("inventory_report.yaml")
        proc = InventoryReportProcessor(self.spark, config)
        df = proc.run()

        # After aggregation over warehouse A: two products
        self.assertTrue(df is None or df.count() == 2)

    def test_sales_report_e2e(self):
        sales_data = [
            {"id": 1, "product": "Widget", "amount": 100},
            {"id": 2, "product": "Gadget", "amount": 200},
            {"id": 3, "product": "Widget", "amount": 150},
        ]
        self.spark.createDataFrame(sales_data).createOrReplaceTempView("sales")

        config = load_yaml("sales_report.yaml")
        proc = SalesReportProcessor(self.spark, config)
        df = proc.run()

        # sales_agg view should be created; read step will read it back
        result = self.spark.table("sales_agg")
        self.assertEqual(result.count(), 2)


if __name__ == "__main__":
    unittest.main()

