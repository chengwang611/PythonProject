# test_spark_standalone.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("spark-test")
    .getOrCreate()
)

print("Spark version:", spark.version)
spark.stop()
