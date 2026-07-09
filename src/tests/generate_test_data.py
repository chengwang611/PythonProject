"""
Script to generate sample parquet test data files.
Run this once to create test data under tests/data/
"""
import os
import sys
from pyspark.sql import SparkSession

# Ensure project root on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def main():
    spark = SparkSession.builder.master("local[2]").appName("generate_test_data").getOrCreate()

    # Customer data
    customer_data = [
        {"customer_id": 1, "name": "Alice", "email": "alice@example.com", "region": "US"},
        {"customer_id": 2, "name": "Bob", "email": "bob@example.com", "region": "EU"},
        {"customer_id": 3, "name": "Charlie", "email": "charlie@example.com", "region": "US"},
        {"customer_id": 1, "name": "Alice", "email": "alice@example.com", "region": "US"},  # duplicate
    ]
    spark.createDataFrame(customer_data).write.mode("overwrite").parquet(
        os.path.join(DATA_DIR, "customers.parquet")
    )
    print(f"Created {DATA_DIR}/customers.parquet")

    # Inventory data
    inventory_data = [
        {"product_id": 1, "product_name": "Widget", "quantity": 100, "warehouse": "A"},
        {"product_id": 2, "product_name": "Gadget", "quantity": 50, "warehouse": "A"},
        {"product_id": 3, "product_name": "Widget", "quantity": 75, "warehouse": "B"},
        {"product_id": 4, "product_name": "Gadget", "quantity": 200, "warehouse": "B"},
    ]
    spark.createDataFrame(inventory_data).write.mode("overwrite").parquet(
        os.path.join(DATA_DIR, "inventory.parquet")
    )
    print(f"Created {DATA_DIR}/inventory.parquet")

    # Sales data
    sales_data = [
        {"id": 1, "product": "Widget", "amount": 100},
        {"id": 2, "product": "Gadget", "amount": 200},
        {"id": 3, "product": "Widget", "amount": 150},
    ]
    spark.createDataFrame(sales_data).write.mode("overwrite").parquet(
        os.path.join(DATA_DIR, "sales.parquet")
    )
    print(f"Created {DATA_DIR}/sales.parquet")

    spark.stop()
    print("Test data generation complete!")


if __name__ == "__main__":
    main()

