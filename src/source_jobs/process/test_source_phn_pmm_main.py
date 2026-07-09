import os
import sys
import json
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

# Ensure the current folder is on sys.path so the local module can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import source_phn_pmm_main as pmm  # noqa: E402  (import after sys.path tweak)


# ---------------------------------------------------------------------------
# Spark fixture for tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("source_phn_pmm_main-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# get_oauth_token
# ---------------------------------------------------------------------------

@patch("source_phn_pmm_main.requests.post")
def test_get_oauth_token_success(mock_post):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "access_token": "TEST_TOKEN",
        "instance_url": "https://instance",
    }
    mock_post.return_value = resp

    token, instance_url = pmm.get_oauth_token(
        "client-id", "client-secret", "https://login"
    )

    assert token == "TEST_TOKEN"
    assert instance_url == "https://instance"
    mock_post.assert_called_once()


@patch("source_phn_pmm_main.requests.post")
def test_get_oauth_token_failure_raises(mock_post):
    resp = MagicMock()
    resp.status_code = 400
    resp.text = "bad request"
    mock_post.return_value = resp

    with pytest.raises(Exception):
        pmm.get_oauth_token("client-id", "client-secret", "https://login")


# ---------------------------------------------------------------------------
# load_and_save_json_as_parquet
# ---------------------------------------------------------------------------

def test_load_and_save_json_as_parquet_creates_parquet_and_df(spark, tmp_path):
    # Arrange: write a tiny JSON file with "items" array
    input_path = tmp_path / "input.json"
    payload = {
        "items": [
            {"productId": "p1", "attributeId": "a1", "value": "v1", "series": "s1"},
            {"productId": "p2", "attributeId": "a2", "value": "v2", "series": "s2"},
        ]
    }
    input_path.write_text(json.dumps(payload))

    output_dir = tmp_path / "out_parquet"

    # Act
    df = pmm.load_and_save_json_as_parquet(
        spark, str(input_path), str(output_dir)
    )

    # Assert: dataframe content + output path exists
    assert df.count() == 2
    assert set(df.columns) == {"productId", "attributeId", "value", "series"}
    assert output_dir.exists()


# ---------------------------------------------------------------------------
# delete_s3_compatible_folder
# ---------------------------------------------------------------------------

@patch("source_phn_pmm_main.boto3.resource")
def test_delete_s3_compatible_folder_deletes_objects(mock_resource):
    mock_bucket = MagicMock()
    mock_obj1 = MagicMock()
    mock_obj2 = MagicMock()
    mock_bucket.objects.filter.return_value = [mock_obj1, mock_obj2]
    mock_resource.return_value.Bucket.return_value = mock_bucket

    pmm.delete_s3_compatible_folder(
        bucket_name="my-bucket",
        prefix="temp/",
        endpoint_url="https://endpoint",
        access_key="ak",
        secret_key="sk",
    )

    mock_resource.assert_called_once()
    mock_bucket.objects.filter.assert_called_once_with(Prefix="temp/")
    assert mock_obj1.delete.called
    assert mock_obj2.delete.called


# ---------------------------------------------------------------------------
# get_attribute_id / get_attribute_value
# ---------------------------------------------------------------------------

@patch("source_phn_pmm_main.requests.get")
def test_get_attribute_id_success(mock_get):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = [
        {"id": 1},
        {"id": "2"},
        {"no_id": 3},  # should be ignored
    ]
    mock_get.return_value = resp

    ids = pmm.get_attribute_id("TOKEN", "https://attr-id-url")

    assert ids == ["1", "2"]
    mock_get.assert_called_once()


@patch("source_phn_pmm_main.requests.get")
def test_get_attribute_id_failure_raises(mock_get):
    resp = MagicMock()
    resp.status_code = 500
    resp.text = "server error"
    mock_get.return_value = resp

    with pytest.raises(Exception):
        pmm.get_attribute_id("TOKEN", "https://attr-id-url")


@patch("source_phn_pmm_main.requests.get")
def test_get_attribute_value_success(mock_get):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"attributeValues": [{"k": "v"}]}
    mock_get.return_value = resp

    values = pmm.get_attribute_value("TOKEN", "https://attr-val-url")

    assert values == [{"k": "v"}]
    mock_get.assert_called_once()


@patch("source_phn_pmm_main.requests.get")
def test_get_attribute_value_failure_raises(mock_get):
    resp = MagicMock()
    resp.status_code = 404
    resp.text = "not found"
    mock_get.return_value = resp

    with pytest.raises(Exception):
        pmm.get_attribute_value("TOKEN", "https://attr-val-url")


# ---------------------------------------------------------------------------
# save_json_on_temp_folder
# ---------------------------------------------------------------------------

@patch("source_phn_pmm_main.delete_s3_compatible_folder")
@patch("source_phn_pmm_main.boto3.client")
def test_save_json_on_temp_folder_puts_object_and_returns_count(
    mock_client, mock_delete
):
    data = [{"a": 1}, {"a": 2}, {"a": 3}]

    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3

    count = pmm.save_json_on_temp_folder(
        data,
        s3_bucket="bucket",
        s3_temp_folder="temp/folder",
        s3_client_id="id",
        s3_client_secret="secret",
        s3_endpoint="https://endpoint",
    )

    # count matches number of items
    assert count == 3

    # cleanup was invoked
    mock_delete.assert_called_once()

    # S3 client was created with expected args
    mock_client.assert_called_once_with(
        "s3",
        endpoint_url="https://endpoint",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
    )

    # And put_object was called with proper Bucket/Key/Body
    assert mock_s3.put_object.called
    _, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == "bucket"
    assert kwargs["Key"].endswith("attributes.json")

    body = json.loads(kwargs["Body"])
    assert body["items"] == data


# ---------------------------------------------------------------------------
# run – orchestration
# ---------------------------------------------------------------------------

@patch("source_phn_pmm_main.load_and_save_json_as_parquet")
@patch("source_phn_pmm_main.save_json_on_temp_folder")
@patch("source_phn_pmm_main.get_attribute_value")
@patch("source_phn_pmm_main.get_attribute_id")
@patch("source_phn_pmm_main.get_oauth_token")
def test_run_happy_path_calls_all_steps(
    mock_get_token,
    mock_get_ids,
    mock_get_values,
    mock_save_json,
    mock_load_parquet,
    spark,
):
    mock_get_token.return_value = ("TOKEN", "https://instance")
    mock_get_ids.return_value = ["1", "2"]
    mock_get_values.return_value = [{"k": "v"}]
    mock_save_json.return_value = 10

    fake_df = MagicMock()
    fake_df.count.return_value = 10
    mock_load_parquet.return_value = fake_df

    pmm.run(
        spark=spark,
        client_id="cid",
        client_secret="csec",
        login_url="https://login",
        object_name="MyObject",
        s3_client_id="ak",
        s3_client_secret="sk",
        s3_endpoint="https://endpoint",
        s3_bucket="bucket",
        s3_temp_folder="temp/folder",
        parquet_output_path="s3a://bucket/out",
        attribute_id_url="https://attr-id-url",
        attribute_value_url="https://attr-val-url",
    )

    mock_get_token.assert_called_once()
    mock_get_ids.assert_called_once()
    mock_get_values.assert_called_once()
    mock_save_json.assert_called_once()
    mock_load_parquet.assert_called_once()


@patch("source_phn_pmm_main.save_json_on_temp_folder")
@patch("source_phn_pmm_main.get_attribute_value")
@patch("source_phn_pmm_main.get_attribute_id")
@patch("source_phn_pmm_main.get_oauth_token")
def test_run_raises_when_no_records(
    mock_get_token,
    mock_get_ids,
    mock_get_values,
    mock_save_json,
    spark,
):
    mock_get_token.return_value = ("TOKEN", "https://instance")
    mock_get_ids.return_value = []
    mock_get_values.return_value = []
    mock_save_json.return_value = 0

    with pytest.raises(Exception):
        pmm.run(
            spark=spark,
            client_id="cid",
            client_secret="csec",
            login_url="https://login",
            object_name="MyObject",
            s3_client_id="ak",
            s3_client_secret="sk",
            s3_endpoint="https://endpoint",
            s3_bucket="bucket",
            s3_temp_folder="temp/folder",
            parquet_output_path="s3a://bucket/out",
            attribute_id_url="https://attr-id-url",
            attribute_value_url="https://attr-val-url",
        )
