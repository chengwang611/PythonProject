"""
Unit tests for etl_util module with code coverage focus.
Tests all major functions with mocking of external dependencies.
"""
import unittest
from unittest.mock import MagicMock, patch, Mock, mock_open
from datetime import datetime, timedelta
import sys
import osdef setUp(self):
    customer_data = [
        {"customer_id": 1, "name": "Alice", ...},
        ...
    ]
    self.spark.createDataFrame(customer_data).createOrReplaceTempView("customers")

# Mock external dependencies BEFORE importing etl_util
sys.modules['dna_common_starter_parent'] = MagicMock()
sys.modules['dna_common_starter_parent.common_data_access'] = MagicMock()
sys.modules['dna_common_starter_parent.common_data_access.scon_utils'] = MagicMock()
sys.modules['dna_common_starter_parent.common_vault'] = MagicMock()
sys.modules['dna_common_starter_parent.common_vault.read_from_vault'] = MagicMock()
sys.modules['dna_common_starter_parent.common_logging'] = MagicMock()
sys.modules['dna_common_starter_parent.common_logging.generic_loggers'] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from source_jobs.process import etl_util
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, IntegerType, StructField, StructType


class TestGetResourcesBase(unittest.TestCase):
    """Test get_resources_base function."""

    def test_get_resources_base_gcc(self):
        """Test GCC environment returns GCC path."""
        result = etl_util.get_resources_base("GCC_PROD")
        self.assertEqual(result, "/app/resources/PROD/GCC/")

    def test_get_resources_base_scc(self):
        """Test SCC environment returns SCC path."""
        result = etl_util.get_resources_base("SCC_PROD")
        self.assertEqual(result, "/app/resources/PROD/SCC/")

    def test_get_resources_base_non_prod(self):
        """Test non-prod environment returns NON_PROD path."""
        result = etl_util.get_resources_base("DEV")
        self.assertEqual(result, "/app/resources/NON_PROD/")


class TestReadYamlFromPath(unittest.TestCase):
    """Test read_yaml_from_path function."""

    @patch("source_jobs.process.etl_util.logger")
    def test_read_yaml_from_path_success(self, mock_logger):
        """Test successful YAML read from path."""
        yaml_content = "key: value\nlist:\n  - item1\n  - item2"
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = etl_util.read_yaml_from_path("/path/to/config.yaml")
            self.assertIsNotNone(result)
            self.assertEqual(result["key"], "value")

    @patch("source_jobs.process.etl_util.logger")
    def test_read_yaml_from_path_file_not_found(self, mock_logger):
        """Test read YAML with file not found."""
        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with self.assertRaises(FileNotFoundError):
                etl_util.read_yaml_from_path("/nonexistent/path.yaml")


class TestGetSparkSessionV2(unittest.TestCase):
    """Test get_spark_session_v2 function."""

    @patch("source_jobs.process.etl_util.logger")
    @patch("source_jobs.process.etl_util.SparkSession")
    def test_get_spark_session_v2_success(self, mock_spark_class, mock_logger):
        """Test successful Spark session creation."""
        mock_builder = MagicMock()
        mock_spark_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        result = etl_util.get_spark_session_v2(
            app_name="test_app",
            master_url="local",
            driver_memory="2g",
            executor_memory="2g",
            executor_cores="2",
            driver_cores="2",
            max_number_of_executors="4",
            s3_key="key",
            s3_password="pwd",
            endpoint="http://localhost:9000",
        )

        self.assertIsNotNone(result)
        mock_logger.info.assert_called()


class TestReplaceBucketKeys(unittest.TestCase):
    """Test replace_bucket_keys function."""

    @patch("source_jobs.process.etl_util.get_config_root")
    @patch("source_jobs.process.etl_util.logger")
    def test_replace_bucket_keys_success(self, mock_logger, mock_config):
        """Test bucket key replacement."""
        mock_config.return_value = {
            "root": {
                "KYC_BUCKET_NAME": "real-kyc-bucket",
                "PIM_TS_BUCKET_NAME": "real-pim-ts",
                "PIM_BUCKET_NAME": "real-pim",
                "PIM_TS_REFERENCE_BUCKET_NAME": "real-pim-ref",
                "UNITHRAX_GAM_BUCKET_NAME1": "real-gam1",
                "UNITHRAX_GAM_BUCKET_NAME2": "real-gam2",
            }
        }

        result = etl_util.replace_bucket_keys("s3a://KYC_BUCKET_NAME/data")
        self.assertEqual(result, "s3a://real-kyc-bucket/data")

    @patch("source_jobs.process.etl_util.get_config_root")
    @patch("source_jobs.process.etl_util.logger")
    def test_replace_bucket_keys_no_match(self, mock_logger, mock_config):
        """Test bucket key replacement with no match."""
        mock_config.return_value = {
            "root": {
                "KYC_BUCKET_NAME": "real-kyc-bucket",
                "PIM_TS_BUCKET_NAME": "real-pim-ts",
                "PIM_BUCKET_NAME": "real-pim",
                "PIM_TS_REFERENCE_BUCKET_NAME": "real-pim-ref",
                "UNITHRAX_GAM_BUCKET_NAME1": "real-gam1",
                "UNITHRAX_GAM_BUCKET_NAME2": "real-gam2",
            }
        }

        result = etl_util.replace_bucket_keys("s3a://some-other-bucket/data")
        self.assertEqual(result, "s3a://some-other-bucket/data")


class TestReadYamlFromS3(unittest.TestCase):
    """Test read_yaml_from_s3 function."""

    @patch("source_jobs.process.etl_util.logger")
    def test_read_yaml_from_s3_success(self, mock_logger):
        """Test successful YAML read from S3."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_row.value = "key: value"
        mock_df.collect.return_value = [mock_row]

        mock_reader = MagicMock()
        mock_spark.read.text.return_value = mock_df

        result = etl_util.read_yaml_from_s3(mock_spark, "s3a://bucket/config.yaml")
        self.assertIsNotNone(result)

    @patch("source_jobs.process.etl_util.logger")
    def test_read_yaml_from_s3_failure(self, mock_logger):
        """Test YAML read from S3 failure."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.text.side_effect = Exception("S3 read error")

        with self.assertRaises(Exception):
            etl_util.read_yaml_from_s3(mock_spark, "s3a://bucket/config.yaml")


class TestValidate(unittest.TestCase):
    """Test validate function."""

    @patch("source_jobs.process.etl_util.logger")
    def test_validate_empty_check_false(self, mock_logger):
        """Test validate with empty_check=False."""
        mock_df = MagicMock(spec=DataFrame)
        # Should not raise any error
        etl_util.validate(mock_df, "TestDF", "False")

    @patch("source_jobs.process.etl_util.logger")
    def test_validate_empty_dataframe_raises(self, mock_logger):
        """Test validate raises when DataFrame is empty."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.rdd.isEmpty.return_value = True

        with self.assertRaises(ValueError) as context:
            etl_util.validate(mock_df, "TestDF", "True")
        self.assertIn("empty", str(context.exception))

    @patch("source_jobs.process.etl_util.logger")
    def test_validate_non_empty_dataframe(self, mock_logger):
        """Test validate with non-empty DataFrame."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.rdd.isEmpty.return_value = False
        # Should not raise
        etl_util.validate(mock_df, "TestDF", "True")


class TestNextDay(unittest.TestCase):
    """Test next_day function."""

    def test_next_day_special_buckets_same_date(self):
        """Test next_day returns same date for special buckets."""
        result = etl_util.next_day("2025-12-03", "s3a://daily_mf_client_holdings_sales/data")
        self.assertEqual(result, "2025-12-03")

    def test_next_day_special_buckets_fund_reference(self):
        """Test next_day with fund reference bucket."""
        result = etl_util.next_day("2025-12-03", "s3a://fund_reference_master_fma/data")
        self.assertEqual(result, "2025-12-03")

    def test_next_day_regular_bucket_increments(self):
        """Test next_day increments for regular buckets."""
        result = etl_util.next_day("2025-12-03", "s3a://regular-bucket/data")
        self.assertEqual(result, "2025-12-04")

    def test_next_day_year_boundary(self):
        """Test next_day handles year boundary."""
        result = etl_util.next_day("2025-12-31", "s3a://regular-bucket/data")
        self.assertEqual(result, "2026-01-01")


class TestReplaceEmptyWithNULLString(unittest.TestCase):
    """Test replace_empty_with_NULL_string function."""

    @patch("source_jobs.process.etl_util.regexp_replace")
    @patch("source_jobs.process.etl_util.col")
    @patch("source_jobs.process.etl_util.when")
    @patch("source_jobs.process.etl_util.trim")
    def test_replace_empty_with_null_string_columns(self, mock_trim, mock_when, mock_col, mock_regexp):
        """Test replace_empty_with_NULL_string processes string columns."""
        mock_df = MagicMock(spec=DataFrame)

        # Create schema with one string field
        mock_field = MagicMock()
        mock_field.name = "name"
        mock_field.dataType = StringType()
        mock_df.schema.fields = [mock_field]

        mock_when.return_value = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = etl_util.replace_empty_with_NULL_string(mock_df)
        self.assertEqual(result, mock_df)

    def test_replace_empty_with_null_string_no_string_columns(self):
        """Test with DataFrame having no string columns."""
        mock_df = MagicMock(spec=DataFrame)
        mock_field = MagicMock()
        mock_field.name = "id"
        mock_field.dataType = IntegerType()
        mock_df.schema.fields = [mock_field]

        result = etl_util.replace_empty_with_NULL_string(mock_df)
        self.assertEqual(result, mock_df)
        # withColumn should not be called
        mock_df.withColumn.assert_not_called()


class TestExecuteSQL(unittest.TestCase):
    """Test execute_sql function."""

    @patch("source_jobs.process.etl_util.logger")
    def test_execute_sql_select_success(self, mock_logger):
        """Test execute_sql with SELECT query."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_spark.sql.return_value = mock_df

        etl_util.execute_sql(mock_spark, "result_view", "SELECT * FROM source")
        mock_spark.sql.assert_called()

    @patch("source_jobs.process.etl_util.logger")
    def test_execute_sql_with_query(self, mock_logger):
        """Test execute_sql with WITH query."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_spark.sql.return_value = mock_df

        etl_util.execute_sql(mock_spark, "result", "WITH cte AS (...) SELECT * FROM cte")
        mock_spark.sql.assert_called()

    @patch("source_jobs.process.etl_util.logger")
    def test_execute_sql_invalid_query(self, mock_logger):
        """Test execute_sql rejects non-SELECT queries."""
        mock_spark = MagicMock(spec=SparkSession)

        with self.assertRaises(ValueError):
            etl_util.execute_sql(mock_spark, "view", "DELETE FROM table")

    @patch("source_jobs.process.etl_util.logger")
    def test_execute_sql_create_view(self, mock_logger):
        """Test execute_sql with CREATE VIEW."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_spark.sql.return_value = mock_df

        etl_util.execute_sql(mock_spark, "view", "CREATE VIEW v AS SELECT *")
        mock_spark.sql.assert_called()


class TestLoadViewsFromPartition(unittest.TestCase):
    """Test load_views_from_partition function."""

    @patch("source_jobs.process.etl_util.replace_bucket_keys")
    @patch("source_jobs.process.etl_util.next_day")
    @patch("source_jobs.process.etl_util.validate")
    @patch("source_jobs.process.etl_util.replace_empty_with_NULL_string")
    @patch("source_jobs.process.etl_util.logger")
    def test_load_views_from_partition_parquet(
        self, mock_logger, mock_replace_empty, mock_validate, mock_next_day, mock_replace_bucket
    ):
        """Test loading parquet file."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["col1", "col2"]

        mock_spark.read.parquet.return_value = mock_df
        mock_replace_empty.return_value = mock_df
        mock_replace_bucket.return_value = "s3a://bucket"
        mock_next_day.return_value = "2025-12-04"

        file_config = [
            {
                "view_name": "TEST_VIEW",
                "path": "/data",
                "format": "parquet",
                "date_partition_key": "date",
                "empty_check": "False"
            }
        ]

        etl_util.load_views_from_partition(
            mock_s3_client, mock_spark, "s3a://bucket", file_config, "2025-12-03"
        )

        mock_spark.read.parquet.assert_called()
        mock_df.createOrReplaceTempView.assert_called_with("TEST_VIEW")

    @patch("source_jobs.process.etl_util.replace_bucket_keys")
    @patch("source_jobs.process.etl_util.validate")
    @patch("source_jobs.process.etl_util.replace_empty_with_NULL_string")
    @patch("source_jobs.process.etl_util.logger")
    def test_load_views_from_partition_csv(
        self, mock_logger, mock_replace_empty, mock_validate, mock_replace_bucket
    ):
        """Test loading CSV file."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["col1", "col2"]

        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read.format.return_value = mock_reader

        mock_replace_empty.return_value = mock_df
        mock_replace_bucket.return_value = "s3a://bucket"

        file_config = [
            {
                "view_name": "TEST_CSV",
                "path": "/data",
                "format": "csv",
                "empty_check": "False"
            }
        ]

        etl_util.load_views_from_partition(
            mock_s3_client, mock_spark, "s3a://bucket", file_config, "2025-12-03"
        )

        mock_spark.read.format.assert_called_with("csv")

    @patch("source_jobs.process.etl_util.replace_bucket_keys")
    @patch("source_jobs.process.etl_util.logger")
    def test_load_views_from_partition_unsupported_format(self, mock_logger, mock_replace_bucket):
        """Test loading unsupported format raises error."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mock_replace_bucket.return_value = "s3a://bucket"

        file_config = [
            {
                "view_name": "TEST",
                "path": "/data",
                "format": "json",
                "empty_check": "False"
            }
        ]

        with self.assertRaises(ValueError):
            etl_util.load_views_from_partition(
                mock_s3_client, mock_spark, "s3a://bucket", file_config, "2025-12-03"
            )


class TestWriteViewToMSSql(unittest.TestCase):
    """Test write_view_to_mssql function."""

    @patch("source_jobs.process.etl_util.lit")
    @patch("source_jobs.process.etl_util.col")
    @patch("source_jobs.process.etl_util.replace_empty_with_NULL_string")
    @patch("source_jobs.process.etl_util.execute_scon_query_df")
    @patch("source_jobs.process.etl_util.write_df_to_scon")
    @patch("source_jobs.process.etl_util.logger")
    def test_write_view_to_mssql_success(
        self, mock_logger, mock_write_scon, mock_execute_scon, mock_replace_empty,
        mock_col_func, mock_lit_func
    ):
        """Test successful write to MSSQL."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["col1", "col2"]
        mock_df.drop.return_value = mock_df

        # Mock withColumn to handle lit() call
        mock_col_value = MagicMock()
        mock_lit_func.return_value = mock_col_value
        mock_col_value.cast.return_value = mock_col_value
        mock_df.withColumn.return_value = mock_df

        mock_table = MagicMock()
        mock_table.name = "test_view"
        mock_spark.catalog.listTables.return_value = [mock_table]
        mock_spark.table.return_value = mock_df

        mock_replace_empty.return_value = mock_df

        mssql_config = {
            "connection_url": "jdbc://host",
            "username": "user",
            "password": "pwd",
            "database": "db",
            "batch_size": 1000
        }

        table_config = {"table_name": "output_table"}

        etl_util.write_view_to_mssql(
            mock_spark, "test_view", mssql_config, table_config, "2025-12-03"
        )

        mock_execute_scon.assert_called_once()
        mock_write_scon.assert_called_once()

    @patch("source_jobs.process.etl_util.logger")
    def test_write_view_to_mssql_invalid_view(self, mock_logger):
        """Test write with invalid view name."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_table = MagicMock()
        mock_table.name = "other_view"
        mock_spark.catalog.listTables.return_value = [mock_table]

        mssql_config = {
            "connection_url": "jdbc://host",
            "username": "user",
            "password": "pwd",
            "database": "db"
        }

        with self.assertRaises(ValueError):
            etl_util.write_view_to_mssql(
                mock_spark, "invalid_view", mssql_config, {}, "2025-12-03"
            )


class TestLoadMSSQLTableAsSparkTempTable(unittest.TestCase):
    """Test load_mssql_table_as_spark_temp_table function."""

    @patch("source_jobs.process.etl_util.execute_scon_query_df")
    @patch("source_jobs.process.etl_util.logger")
    def test_load_mssql_table_success(self, mock_logger, mock_execute_scon):
        """Test successful load from MSSQL."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_execute_scon.return_value = mock_df

        mssql_config = {
            "connection_url": "jdbc://host",
            "username": "user",
            "password": "pwd",
            "database": "db"
        }

        result = etl_util.load_mssql_table_as_spark_temp_table(
            mock_spark, mssql_config, "source_table", "2025-12-03", "temp_view"
        )

        mock_execute_scon.assert_called_once()
        mock_df.createOrReplaceTempView.assert_called_with("temp_view")
        self.assertEqual(result, mock_df)


class TestLoadMSSQLTableAsSparkTempTableNative(unittest.TestCase):
    """Test load_mssql_table_as_spark_temp_table_native function."""

    def test_load_mssql_table_native_success(self):
        """Test native JDBC load from MSSQL."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)

        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read.format.return_value = mock_reader

        mssql_config = {
            "connection_url": "jdbc://host",
            "username": "user",
            "password": "pwd",
            "database": "db"
        }

        result = etl_util.load_mssql_table_as_spark_temp_table_native(
            mock_spark, mssql_config, "source_table", "2025-12-03", "temp_view"
        )

        mock_spark.read.format.assert_called_with("jdbc")
        mock_df.createOrReplaceTempView.assert_called_with("temp_view")
        self.assertEqual(result, mock_df)


class TestProcessSteps(unittest.TestCase):
    """Test process_steps function."""

    @patch("source_jobs.process.etl_util.load_views_from_partition")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_load_views(self, mock_logger, mock_load_views):
        """Test process_steps with load_views action."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)

        config = {
            "steps": [
                {
                    "function": "load_views",
                    "s3_base_path": "s3a://bucket",
                    "files": []
                }
            ]
        }

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, {}, {}, {}
        )

        mock_load_views.assert_called_once()

    @patch("source_jobs.process.etl_util.load_mssql_table_as_spark_temp_table")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_load_table(self, mock_logger, mock_load_table):
        """Test process_steps with load_table action."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mssql_config = {"connection_url": "jdbc://host"}

        config = {
            "steps": [
                {
                    "function": "load_table",
                    "table": "source",
                    "temp_table": "temp"
                }
            ]
        }

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, mssql_config, {}, {}
        )

        mock_load_table.assert_called_once()

    @patch("source_jobs.process.etl_util.load_mssql_table_as_spark_temp_table_native")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_load_table_native(self, mock_logger, mock_load_native):
        """Test process_steps with native JDBC load."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mssql_config = {"connection_url": "jdbc://host"}

        config = {
            "steps": [
                {
                    "function": "load_table",
                    "table": "source",
                    "temp_table": "temp"
                }
            ]
        }

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, mssql_config, {}, {},
            use_native_table_loads=True
        )

        mock_load_native.assert_called_once()

    @patch("source_jobs.process.etl_util.execute_sql")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_execute_sql(self, mock_logger, mock_execute_sql):
        """Test process_steps with execute_sql action."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)

        config = {
            "steps": [
                {
                    "function": "execute_sql",
                    "view_name": "result",
                    "sql": "SELECT * FROM source"
                }
            ]
        }

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, {}, {}, {}
        )

        mock_execute_sql.assert_called_once()

    @patch("source_jobs.process.etl_util.execute_sql")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_execute_sql_by_name(self, mock_logger, mock_execute_sql):
        """Test process_steps with execute_sql_by_name action."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)

        query_dict = {"QUERY_1": "SELECT * FROM source WHERE id > 10"}

        config = {
            "steps": [
                {
                    "function": "execute_sql_by_name",
                    "view_name": "result",
                    "sql": "QUERY_1"
                }
            ]
        }

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, {}, query_dict, {}
        )

        mock_execute_sql.assert_called_once()

    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_execute_pyspark_function(self, mock_logger):
        """Test process_steps with custom function."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)
        mock_func = MagicMock()

        config = {
            "steps": [
                {
                    "function": "execute_pyspark_function_by_name",
                    "function_name": "transform",
                    "temp_table": "temp"
                }
            ]
        }

        functions = {"transform": mock_func}

        etl_util.process_steps(
            mock_s3_client, mock_spark, "2025-12-03", config, {}, {}, functions
        )

        mock_func.assert_called_once()

    @patch("source_jobs.process.etl_util.logger")
    def test_process_steps_unsupported_function(self, mock_logger):
        """Test process_steps with unsupported function."""
        mock_s3_client = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)

        config = {
            "steps": [
                {
                    "function": "unsupported_func"
                }
            ]
        }

        with self.assertRaises(ValueError):
            etl_util.process_steps(
                mock_s3_client, mock_spark, "2025-12-03", config, {}, {}, {}
            )


class TestProcessConfigs(unittest.TestCase):
    """Test process_configs function."""

    @patch("source_jobs.process.etl_util.boto3.client")
    @patch("source_jobs.process.etl_util.read_yaml_from_path")
    @patch("source_jobs.process.etl_util.get_spark_session_v2")
    @patch("source_jobs.process.etl_util.get_config_root")
    @patch("source_jobs.process.etl_util.logger")
    def test_process_configs_success(
        self, mock_logger, mock_get_config, mock_spark_v2, mock_read_yaml, mock_boto3
    ):
        """Test process_configs returns correct values."""
        mock_get_config.return_value = {
            "root": {
                "MASTER_URL": "spark://master",
                "SPARK_12_DRIVER_MEMORY": "2g",
                "SPARK_12_EXECUTOR_MEMORY": "2g",
                "SPARK_M_D_I_CORES": "2",
                "SPARK_M_E_I_CORES": "2",
                "MAX_NUMBER_OF_EXECUTORS": "4",
                "AWS_ACCESS_KEY_ID": "key",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "END_POINT": "http://localhost:9000",
                "mssql_connection_url": "jdbc://host",
                "mssql_username": "user",
                "mssql_pwd": "pwd",
                "mssql_db": "db",
                "mssql_batch_size": "1000"
            }
        }

        mock_spark = MagicMock(spec=SparkSession)
        mock_spark_v2.return_value = mock_spark
        mock_read_yaml.return_value = {"test": "config"}
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3

        spark, mssql_config, yaml_config, s3_client = etl_util.process_configs(
            "test_job", "GCC", "config.yaml", "2025-12-03"
        )

        self.assertEqual(spark, mock_spark)
        self.assertIn("connection_url", mssql_config)
        self.assertEqual(mssql_config["username"], "user")
        self.assertEqual(yaml_config, {"test": "config"})
        self.assertEqual(s3_client, mock_s3)


if __name__ == "__main__":
    unittest.main()

