"""Example runner showing how to ingest using the provided helpers.

Not intended to be run as-is in CI; it's an example you can adapt.
"""
from pyspark.sql import SparkSession
from .auth import OAuth2Client
from .salesforce_ingest import SalesforceIngestor
from .s3_util import write_df_to_s3_parquet


def example_salesforce_snapshot(oauth_conf, sf_conf, s3_path):
    spark = SparkSession.builder.appName("sf-ingest-example").getOrCreate()
    oauth = OAuth2Client(oauth_conf["token_url"], oauth_conf["client_id"], oauth_conf["client_secret"])
    ingestor = SalesforceIngestor(sf_conf["instance_url"], oauth)

    soql = "SELECT Id, Name, CreatedDate FROM Account"
    records = ingestor.query_all(soql)
    # convert records to dataframe
    df = spark.createDataFrame(records)
    # partition by ingestion_date which the caller can compute; simple example:
    df = df.withColumn("ingestion_date", spark.sql("select current_date() as d").select("d").first()[0])
    write_df_to_s3_parquet(df, s3_path, partition_cols=["ingestion_date"], mode="overwrite")
    spark.stop()


if __name__ == "__main__":
    print("This runner is an example. Configure credentials and call example_salesforce_snapshot()")

