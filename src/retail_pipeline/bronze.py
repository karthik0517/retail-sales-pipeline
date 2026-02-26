import argparse
from pyspark.sql import SparkSession

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--input_path", required=True)

    args = parser.parse_args()

    catalog = args.catalog
    schema = args.schema
    input_path = args.input_path

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    print(f"Reading from:- {input_path}")
    print(f"Writing to: {catalog}.{schema}.orders_bronze")

    df = spark.read.option("header", "true").csv(input_path)

    df.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"{catalog}.{schema}.orders_bronze")

    print("Bronze ingestion complete.")