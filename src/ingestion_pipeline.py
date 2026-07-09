# ingestion_pipeline.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper

def read_csv_from_s3(spark: SparkSession, path: str, header: bool = True, delimiter: str = ",") -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.
    - All columns loaded as strings (schema inference disabled)
    """
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", "false")
        .option("delimiter", delimiter)
        .csv(path)
    )

def apply_generic_cleaning(df: DataFrame) -> DataFrame:
    """
    Clean ingestion DataFrame:
    - Trim all string columns
    - Uppercase 'country_code' if present
    - Filter out rows where 'id' is null
    """
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

    cleaned = df
    for c in string_cols:
        cleaned = cleaned.withColumn(c, trim(col(c)))

    if "country_code" in cleaned.columns:
        cleaned = cleaned.withColumn("country_code", upper(col("country_code")))

    if "id" in cleaned.columns:
        cleaned = cleaned.filter(col("id").isNotNull())

    return cleaned
