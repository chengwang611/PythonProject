"""Small helper to save data to S3 as parquet.

The helpers expect a pyspark DataFrame and a target s3 path like s3a://bucket/prefix/
Partitioning by ingestion_date is handled by the caller or this helper.
"""
from pyspark.sql import DataFrame


def write_df_to_s3_parquet(df: DataFrame, s3_path: str, partition_cols=None, mode: str = "append") -> None:
    writer = df.write.format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.mode(mode).save(s3_path)

