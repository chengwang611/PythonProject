import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

import etl_util


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("etl_util_tests")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_next_day_normal_path():
    d = "2025-11-25"
    path = "s3a://some-bucket/some/path"
    assert etl_util.next_day(d, path) == "2025-11-26"


def test_next_day_bucket_no_plus():
    d = "2025-11-25"
    path = "s3a://daily_mf_client_holdings_sales/some/path"
    # for these paths we expect SAME date
    assert etl_util.next_day(d, path) == d


def test_validate_non_empty(spark):
    df = spark.createDataFrame([Row(x=1)])
    # should not raise
    etl_util.validate(df, "test_df", "True")


def test_validate_empty_raises(spark):
    df = spark.createDataFrame([], "x INT")
    with pytest.raises(ValueError):
        etl_util.validate(df, "empty_df", "True")


def test_replace_empty_with_NULL_string_basic(spark):
    schema = StructType(
        [
            StructField("a", StringType(), True),
            StructField("b", StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("NULL", "value"),
            ("null", "another"),
            ("  ", "   "),
        ],
        schema=schema,
    )

    out = etl_util.replace_empty_with_NULL_string(df).collect()

    # after replacement, a[0] and a[1] should be None, a[2] None; b[2] None
    assert out[0]["a"] is None
    assert out[1]["a"] is None
    assert out[2]["a"] is None
    assert out[2]["b"] is None


def test_execute_sql_rejects_update(monkeypatch, spark):
    # we only verify the guard – if SQL starts with UPDATE it should raise
    with pytest.raises(ValueError):
        etl_util.execute_sql(spark, "TEST_VIEW", "UPDATE table SET x = 1")


def test_execute_sql_creates_view(spark):
    # create a simple source DF & view
    df = spark.createDataFrame([Row(x=1), Row(x=2)])
    df.createOrReplaceTempView("SRC")

    etl_util.execute_sql(
        spark,
        "DST",
        "SELECT x FROM SRC WHERE x = 2",
    )

    rows = spark.sql("SELECT * FROM DST").collect()
    assert len(rows) == 1
    assert rows[0]["x"] == 2
