import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver_schema", required=True)
    parser.add_argument("--gold_schema", required=True)

    args = parser.parse_args()

    catalog = args.catalog
    silver_schema = args.silver_schema
    gold_schema = args.gold_schema

    silver_table = f"{catalog}.{silver_schema}.orders_silver"
    gold_table = f"{catalog}.{gold_schema}.region_sales_summary"

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    print(f"Reading from: {silver_table}")
    print(f"Writing to: {gold_table}")

    df = spark.table(silver_table)

    # Business aggregation
    gold_df = (
        df.groupBy("region")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value")
        )
    )

    # Gold usually overwrite (fully recomputed)
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )

    print("Gold aggregation complete.")