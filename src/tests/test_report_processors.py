"""
Unit tests for report processors: SalesReportProcessor, InventoryReportProcessor, CustomerReportProcessor.
Tests include query name lookup, temp view registration, and step execution.
"""
import os
import sys
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Add the parent directory to the path to import the source_jobs modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.base_report import BaseReportProcessor
from etls.reports.sales_report import SalesReportProcessor
from etls.reports.inventory_report import InventoryReportProcessor
from etls.reports.customer_report import CustomerReportProcessor

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestBaseReportProcessor(unittest.TestCase):
    """Test the BaseReportProcessor query execution and temp view registration."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("report_processor_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Clear any existing temp views before each test."""
        self.spark.sql("DROP VIEW IF EXISTS test_view")
        self.spark.sql("DROP VIEW IF EXISTS users_view")

    def test_execute_query_step_with_named_query(self):
        """Test execute_query_step with a named query from the queries dict."""
        # Create a test processor with named queries
        class TestProcessor(BaseReportProcessor):
            queries = {
                "get_users": "SELECT 1 as id, 'Alice' as name",
            }

        processor = TestProcessor(self.spark, [])
        step = {"query": "get_users", "view": "users_view"}
        df = processor.execute_query_step(step)

        # Verify the result
        self.assertEqual(df.count(), 1)
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)

        # Verify temp view was registered
        result = self.spark.sql("SELECT * FROM users_view")
        self.assertEqual(result.count(), 1)

    def test_execute_query_step_with_raw_sql(self):
        """Test execute_query_step with raw SQL string."""
        processor = BaseReportProcessor(self.spark, [])
        step = {"query": "SELECT 1 as id, 'Bob' as name", "view": "test_view"}
        df = processor.execute_query_step(step)

        # Verify the result
        self.assertEqual(df.count(), 1)
        rows = df.collect()
        self.assertEqual(rows[0]["name"], "Bob")

        # Verify temp view was registered
        result = self.spark.sql("SELECT * FROM test_view")
        self.assertEqual(result.count(), 1)

    def test_execute_query_step_without_view_name(self):
        """Test execute_query_step without explicit view registration."""
        processor = BaseReportProcessor(self.spark, [])
        step = {"query": "SELECT 1 as id"}
        df = processor.execute_query_step(step)

        # Should return a DataFrame without error
        self.assertEqual(df.count(), 1)

    def test_execute_query_step_query_not_found(self):
        """Test execute_query_step raises KeyError when query name not found."""
        class TestProcessor(BaseReportProcessor):
            queries = {"get_users": "SELECT 1"}

        processor = TestProcessor(self.spark, [])
        step = {"query": "nonexistent_query"}

        with self.assertRaises(KeyError):
            processor.execute_query_step(step)

    def test_execute_query_step_missing_query_field(self):
        """Test execute_query_step raises ValueError when 'query' field is missing."""
        processor = BaseReportProcessor(self.spark, [])
        step = {"view": "test_view"}  # Missing 'query' field

        with self.assertRaises(ValueError):
            processor.execute_query_step(step)


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

