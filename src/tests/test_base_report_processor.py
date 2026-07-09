"""
Unit tests for BaseReportProcessor: query execution and temp view registration.
"""
import unittest
from pyspark.sql import SparkSession

import sys
import os
# Ensure the project root is on sys.path so `source_jobs` package can be imported when tests
# are executed directly (unittest discovery may import modules as top-level).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etls.reports.base_report import BaseReportProcessor


class TestBaseReportProcessor(unittest.TestCase):
    """Test the BaseReportProcessor query execution and temp view registration."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("report_processor_base_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Clear any existing temp views before each test."""
        try:
            self.spark.sql("DROP VIEW IF EXISTS test_view")
            self.spark.sql("DROP VIEW IF EXISTS users_view")
        except Exception:
            pass

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


if __name__ == "__main__":
    unittest.main()
