# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# please write a function the loads a yaml config file and prints its contents, it including a list of steps which incluing  query name and query spark sql

import yaml
def load_and_print_yaml_config(file_path):

    """
    Load a YAML config file and print its contents.
    The YAML file should contain a list of steps, each with a query name and Spark SQL.
    :param file_path:
    :return: a list of steps with query name and query spark sql  """
    with open(file_path, 'r') as f:
        config_data = yaml.safe_load(f)
    for step in config_data.get("steps", []):
        query_name = step.get("query_name")
        query_sql = step.get("query_spark_sql")
        print(f"Query Name: {query_name}")
        print(f"Query Spark SQL: {query_sql}")
        print("-----")






# please create a function  named process that using a  list of steps  the steps including a few type1 ) load parquet file from S3 as spark view 2) loading from sql database table and 3) excuting query by query name  4) write a vew to sql table

from pyspark.sql import SparkSession

def process(spark: SparkSession, steps: list):
    """
    Process a list of steps including:
    1) Load parquet file from S3 as Spark view
    2) Load from SQL database table
    3) Execute query by query name
    4) Write a view to SQL table
    :param spark: SparkSession
    :param steps: List of steps to process
    :return: None
    """
    for step in steps:
        step_type = step.get("type")
        if step_type == "load_parquet":
            path = step.get("path")
            view_name = step.get("view_name")
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(view_name)
            print(f"Loaded parquet from {path} as view {view_name}")
        elif step_type == "load_sql_table":
            table_name = step.get("table_name")
            view_name = step.get("view_name")
            df = spark.read.format("jdbc").option("url", step.get("jdbc_url")) \
                .option("dbtable", table_name) \
                .option("user", step.get("user")) \
                .option("password", step.get("password")).load()
            df.createOrReplaceTempView(view_name)
            print(f"Loaded SQL table {table_name} as view {view_name}")
        elif step_type == "execute_query":
            query_name = step.get("query_name")
            query_sql = step.get("query_spark_sql")
            result_df = spark.sql(query_sql)
            result_df.createOrReplaceTempView(query_name)
            print(f"Executed query {query_name}")
        elif step_type == "write_sql_table":
            view_name = step.get("view_name")
            table_name = step.get("table_name")
            df = spark.table(view_name)
            df.write.format("jdbc").option("url", step.get("jdbc_url")) \
                .option("dbtable", table_name) \
                .option("user", step.get("user")) \
                .option("password", step.get("password")).mode("overwrite").save()
            print(f"Wrote view {view_name} to SQL table {table_name}")



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
