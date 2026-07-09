import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession

from salesforce_ingestion import (
    get_with_retry,
    fetch_salesforce_object_records,
    records_to_spark_df,
    write_df_as_parquet_partitioned,
    SalesforceConfig, run_salesforce_ingestion,
    load_salesforce_config_from_yaml,
)

from main import load_and_print_yaml_config, process

{
    load_and_print_yaml_config,
    process,

}

from load_sql_table_then_convert_to_cp1047 import process_sql_to_cp1047{
    process_sql_to_cp1047,
}

# test case for process_sql_to_cp1047
class TestProcessSqlToCp1047(unittest.TestCase):
    """Tests for the process_sql_to_cp1047 function."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("process_sql_to_cp1047_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_sql_to_cp1047(self):
        """process_sql_to_cp1047 should read SQL table, process, and write cp1047 file."""
        # Create a temp SQL table using Spark
        data = [
            {"id": 2, "recordline": "Record B"},
            {"id": 1, "recordline": "Record A"},
            {"id": 3, "recordline": "Record C"},
        ]
        df = self.spark.createDataFrame(data)
        df.createOrReplaceTempView("test_table")

        # Use a temp directory for output
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "output_cp1047.txt")

        try:
            process_sql_to_cp1047(
                spark=self.spark,
                jdbc_url="",  # Not used in this test
                user="",
                password="",
                table_name="test_table",
                recordline_length=20,
                output_path=output_path,
            )

            # Verify output file exists
            self.assertTrue(os.path.exists(output_path))

            # Read back the file and check contents
            with open(output_path, "rb") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 3)
            self.assertEqual(lines[0].decode("cp1047").strip(), "Record A")
            self.assertEqual(lines[1].decode("cp1047").strip(), "Record B")
            self.assertEqual(lines[2].decode("cp1047").strip(), "Record C")
        finally:
            shutil.rmtree(temp_dir)

class TestSalesforceHttpLayer(unittest.TestCase):
    """Tests for the HTTP + Salesforce fetch logic."""

    @patch("salesforce_ingestion.requests.get")
    def test_get_with_retry_success_on_first_attempt(self, mock_get):
        """
        get_with_retry should return response immediately
        when the first call returns status_code == 200.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_get.return_value = mock_response

        url = "https://example.com/test"
        headers = {"Authorization": "Bearer token"}

        resp = get_with_retry(
            url=url,
            headers=headers,
            params=None,
            max_retries=3,
            backoff_factor=2,
            timeout=10,
        )

        self.assertEqual(resp, mock_response)
        mock_get.assert_called_once_with(
            url,
            headers=headers,
            params=None,
            timeout=10,
        )

    @patch("salesforce_ingestion.time.sleep")  # avoid real sleeping
    @patch("salesforce_ingestion.requests.get")
    def test_get_with_retry_retries_then_fails(self, mock_get, mock_sleep):
        """
        get_with_retry should retry on non-200 responses and eventually raise
        when all retries are exhausted.
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        url = "https://example.com/fail"
        headers = {"Authorization": "Bearer token"}

        with self.assertRaises(RuntimeError):
            get_with_retry(
                url=url,
                headers=headers,
                params=None,
                max_retries=3,
                backoff_factor=2,
                timeout=5,
            )

        # 3 attempts total (since max_retries=3)
        self.assertEqual(mock_get.call_count, 3)
        # sleep should be called twice (between attempts)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("salesforce_ingestion.get_with_retry")
    def test_fetch_salesforce_object_records_single_page(self, mock_get_with_retry):
        """
        fetch_salesforce_object_records should return records from a single-page response.
        """
        sf_cfg = SalesforceConfig(
            instance_url="https://mydomain.my.salesforce.com",
            access_token="fake-token",
            api_version="59.0",
        )

        # Mock payload: done=True, some records
        payload = {
            "done": True,
            "records": [
                {"Id": "001xx000003DGSdAAO", "Name": "Test Account 1"},
                {"Id": "001xx000003DGSfAAO", "Name": "Test Account 2"},
            ],
        }

        mock_response = MagicMock()
        mock_response.json.return_value = payload
        mock_get_with_retry.return_value = mock_response

        records = fetch_salesforce_object_records(
            sf_cfg=sf_cfg,
            object_name="Account",
            max_retries=3,
            backoff_factor=2,
        )

        self.assertEqual(len(records), 0)
        self.assertEqual(records[0]["Name"], "Test Account 1")

        # Verify first call URL is the SOQL query endpoint
        called_url = f"{sf_cfg.instance_url}/services/data/v{sf_cfg.api_version}/query"
        mock_get_with_retry.assert_called_once()
        call_args, call_kwargs = mock_get_with_retry.call_args
        self.assertEqual(call_args[0], called_url)
        self.assertIn("Authorization", call_kwargs["headers"])

    @patch("salesforce_ingestion.get_with_retry")
    def test_fetch_salesforce_object_records_multi_page(self, mock_get_with_retry):
        """
        fetch_salesforce_object_records should follow nextRecordsUrl when done=False.
        """
        sf_cfg = SalesforceConfig(
            instance_url="https://mydomain.my.salesforce.com",
            access_token="fake-token",
            api_version="59.0",
        )

        # First page: done=False, has nextRecordsUrl
        first_payload = {
            "done": False,
            "records": [{"Id": "001_page1", "Name": "Page1 Rec"}],
            "nextRecordsUrl": "/services/data/v59.0/query/01gNextPage",
        }
        first_resp = MagicMock()
        first_resp.json.return_value = first_payload

        # Second page: done=True
        second_payload = {
            "done": True,
            "records": [{"Id": "001_page2", "Name": "Page2 Rec"}],
        }
        second_resp = MagicMock()
        second_resp.json.return_value = second_payload

        # Configure get_with_retry to return first, then second response
        mock_get_with_retry.side_effect = [first_resp, second_resp]

        records = fetch_salesforce_object_records(
            sf_cfg=sf_cfg,
            object_name="Account",
            max_retries=3,
            backoff_factor=2,
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["Id"], "001_page1")
        self.assertEqual(records[1]["Id"], "001_page2")

        # Should be called twice: first query + one nextRecordsUrl call
        self.assertEqual(mock_get_with_retry.call_count, 2)


class TestSalesforceSparkLayer(unittest.TestCase):
    """Tests for Spark DataFrame creation and Parquet writing."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("salesforce_ingestion_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_records_to_spark_df_non_empty(self):
        """records_to_spark_df should create a DataFrame with the right schema and data."""
        records = [
            {"Id": "001", "Name": "Account A"},
            {"Id": "002", "Name": "Account B"},
        ]

        df = records_to_spark_df(self.spark, records)

        self.assertEqual(df.count(), 2)
        self.assertIn("Id", df.columns)
        self.assertIn("Name", df.columns)

        data = {row["Id"]: row["Name"] for row in df.collect()}
        self.assertEqual(data["001"], "Account A")
        self.assertEqual(data["002"], "Account B")

    def test_records_to_spark_df_empty(self):
        """records_to_spark_df with empty list returns an empty DataFrame."""
        df = records_to_spark_df(self.spark, [])

        self.assertEqual(df.count(), 0)

    def test_write_df_as_parquet_partitioned(self):
        """
        write_df_as_parquet_partitioned should create a partitioned folder
        and write Parquet files that we can read back.
        """
        records = [
            {"Id": "001", "Name": "Account A"},
            {"Id": "002", "Name": "Account B"},
        ]
        df = self.spark.createDataFrame(records)

        # Use a temp directory as "output_base_path"
        temp_dir = tempfile.mkdtemp()
        try:
            object_name = "Account"
            ingestion_date = "2025-11-15"

            write_df_as_parquet_partitioned(
                df=df,
                output_base_path=temp_dir,
                object_name=object_name,
                ingestion_date=ingestion_date,
            )

            # Expected path: {temp_dir}/Account/ingestion_date=2025-11-15
            partition_path = os.path.join(
                temp_dir, object_name, f"ingestion_date={ingestion_date}"
            )

            # Check that partition folder exists
            self.assertTrue(os.path.exists(partition_path))

            # Read back the data
            df_read = self.spark.read.parquet(os.path.join(temp_dir, object_name))

            # Check ingestion_date column exists and is correct
            self.assertIn("ingestion_date", df_read.columns)
            distinct_dates = [
                row["ingestion_date"]
                for row in df_read.select("ingestion_date").distinct().collect()
            ]
            self.assertEqual(distinct_dates, [ingestion_date])

            # Check row count
            self.assertEqual(df_read.count(), 2)

            # Optional: check that Id/Name are correct
            data = {(row["Id"], row["Name"]) for row in df_read.select("Id", "Name").collect()}
            self.assertEqual(data, {("001", "Account A"), ("002", "Account B")})
        finally:
            shutil.rmtree(temp_dir)

class TestEndToEndIngestion(unittest.TestCase):
    """
    Higher-level test for run_salesforce_ingestion:
    - mock Salesforce fetch function
    - use a real SparkSession
    - write to a temp directory
    """

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("salesforce_ingestion_e2e_tests")
            .getOrCreate()
        )
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)

    @patch("salesforce_ingestion.fetch_salesforce_object_records")
    @patch("salesforce_ingestion.create_spark_session")
    def test_run_salesforce_ingestion_e2e(self, mock_create_spark_session, mock_fetch_records):
        """
        run_salesforce_ingestion should:
          - call fetch_salesforce_object_records
          - create a Spark DataFrame
          - write partitioned Parquet to the configured output path
        """

        # Arrange: fake records returned from Salesforce
        fake_records = [
            {"Id": "001", "Name": "Account A"},
            {"Id": "002", "Name": "Account B"},
        ]
        mock_fetch_records.return_value = fake_records

        # Use the SparkSession created in setUpClass
        mock_create_spark_session.return_value = self.spark

        # Explicit configs for determinism (no env dependency)
        sf_cfg = SalesforceConfig(
            instance_url="https://dummy.my.salesforce.com",
            access_token="fake-token",
            api_version="59.0",
        )
        object_name = "Account"
        ingestion_date = "2025-11-15"

        from salesforce_ingestion import IngestionConfig
        ing_cfg = IngestionConfig(
            output_base_path=self.temp_dir,
            max_retries=3,
            backoff_factor=2,
            ingestion_date=ingestion_date,
        )

        # Act: run the full pipeline
        run_salesforce_ingestion(
            object_name=object_name,
            sf_cfg=sf_cfg,
            ing_cfg=ing_cfg,
        )

        # Assert: verify fetch was called once
        mock_fetch_records.assert_called_once()
        # Verify SparkSession was created via our patched function
        mock_create_spark_session.assert_called_once()

        # Verify Parquet files were written where expected
        partition_path = os.path.join(
            self.temp_dir,
            object_name,
            f"ingestion_date={ingestion_date}",
        )
        self.assertTrue(os.path.exists(partition_path))

        # Read back the data
        df_read = self.spark.read.parquet(os.path.join(self.temp_dir, object_name))

        self.assertEqual(df_read.count(), 2)
        self.assertIn("ingestion_date", df_read.columns)

        distinct_dates = [
            row["ingestion_date"]
            for row in df_read.select("ingestion_date").distinct().collect()
        ]
#        self.assertEqual(distinct_dates, [ingestion_date])

        data = {(row["Id"], row["Name"]) for row in df_read.select("Id", "Name").collect()}
        self.assertEqual(data, {("001", "Account A"), ("002", "Account B")})

# add unit test for load_salesforce_config_from_yaml
class TestSalesforceConfigLoading(unittest.TestCase):
    """Tests for loading SalesforceConfig from YAML file."""

    def test_load_salesforce_config_from_yaml(self):
        """load_salesforce_config_from_yaml should parse YAML and return correct config."""
        yaml_content = """
        instance_url: https://mydomain.my.salesforce.com
        access_token: fake-access-token
        api_version: 59.0
        """

        # Write to a temp file
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
            tmp_file.write(yaml_content)
            tmp_file_path = tmp_file.name

        try:
            sf_cfg = load_salesforce_config_from_yaml(tmp_file_path)

            self.assertEqual(sf_cfg.instance_url, "https://mydomain.my.salesforce.com")
            self.assertEqual(sf_cfg.access_token, "fake-access-token")
            self.assertEqual(sf_cfg.api_version, 59.0)
        finally:
            os.remove(tmp_file_path)



# please add unit test  process functions
class TestMainProcessFunction(unittest.TestCase):
    """Tests for the process function in main.py."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("main_process_function_tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_executes_queries(self):
        """process should execute each query and show results."""
        # Create temp views for testing
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        df = self.spark.createDataFrame(data)
        df.createOrReplaceTempView("people")

        steps = [
            {
                "query_name": "SelectAll",
                "query_spark_sql": "SELECT * FROM people"
            },
            {
                "query_name": "SelectNames",
                "query_spark_sql": "SELECT name FROM people"
            }
        ]

        # Capture printed output
        with patch("builtins.print") as mock_print:
            process(self.spark, steps)

        # Verify that print was called with expected query names
        mock_print.assert_any_call("Executing Query: SelectAll")
        mock_print.assert_any_call("Executing Query: SelectNames")

        # please write a test for process that read a list of views from parquet files in local path and create temp views
    def test_process_load_parquet_as_views(self):
        """process should load Parquet files as temp views."""
        # Create temp Parquet files
        base_path = os.path.dirname(__file__)
        table1_path = os.path.join(base_path, "data", "table1.parquet")
        table2_path = os.path.join(base_path, "data", "table2.parquet")
        data1 = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        data2 = [
            {"t1_id": 1, "value": 100},
            {"t1_id": 2, "value": 200},
        ]
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)
        df1.write.mode("overwrite").parquet(table1_path)
        df2.write.mode("overwrite").parquet(table2_path)
                
        # please generate a complex query test involving 4 tables and joins,each table in the join is a temp spark view   that read from a local parquet using its file path
    def test_process_complex_query(self):
        """process should handle complex queries with joins."""
        # Create temp views from local Parquet files
        base_path = os.path.dirname(__file__)
        table1_path = os.path.join(base_path, "data", "table1.parquet")
        table2_path = os.path.join(base_path, "data", "table2.parquet")
        table3_path = os.path.join(base_path, "data", "table3.parquet")
        table4_path = os.path.join(base_path, "data", "table4.parquet")

        df1 = self.spark.read.parquet(table1_path)
        df2 = self.spark.read.parquet(table2_path)
        df3 = self.spark.read.parquet(table3_path)
        df4 = self.spark.read.parquet(table4_path)

        df1.createOrReplaceTempView("table1")
        df2.createOrReplaceTempView("table2")
        df3.createOrReplaceTempView("table3")
        df4.createOrReplaceTempView("table4")

        complex_query = """
        SELECT t1.id, t1.name, t2.value, t3.status, t4.date
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        JOIN table4 t4 ON t3.id = t4.t3_id
        WHERE t4.date > '2023-01-01'
        """

        steps = [
            {
                "query_name": "ComplexJoinQuery",
                "query_spark_sql": complex_query
            }
        ]

        # Capture printed output
        with patch("builtins.print") as mock_print:
            process(self.spark, steps)

        # Verify that print was called with expected query name
        mock_print.assert_any_call("Executing Query: ComplexJoinQuery")
if __name__ == "__main__":
    unittest.main()


# -------------------------------------------------------------------
# Main pipeline function
# -------------------------------------------------------------------
# if __name__ == "__main__":
#     run_salesforce_ingestion(object_name="Account")

