# Write pytest unit tests for ingestion_pipeline.py.
# Use a local SparkSession fixture.
# Test:
# - trimming of string columns
# - uppercasing country_code
# - filtering rows where id is null
# - behavior when a column is missing
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from ingestion_pipeline import read_csv_from_s3, apply_generic_cleaning

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for unit tests."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("ingestion_pipeline_tests") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_apply_generic_cleaning_trims_strings(spark):
    data = [
        Row(id=" 1 ", name=" Alice ", country_code=" us "),
        Row(id=" 2 ", name=" Bob ", country_code=" ca ")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = apply_generic_cleaning(df)

    result = cleaned_df.collect()
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Alice"
    assert result[0]["country_code"] == "US"
    assert result[1]["id"] == "2"
    assert result[1]["name"] == "Bob"
    assert result[1]["country_code"] == "CA"

def test_apply_generic_cleaning_filters_null_id(spark):
    data = [
        Row(id="1", name="Alice"),
        Row(id=None, name="Bob"),
        Row(id="3", name="Charlie")
    ]
    df = spark.createDataFrame(data)
    cleaned_df = apply_generic_cleaning(df)

    result = cleaned_df.collect()
    assert len(result) == 2
    assert all(row["id"] is not None for row in result)