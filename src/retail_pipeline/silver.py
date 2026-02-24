import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze_schema", required=True)
    parser.add_argument("--silver_schema", required=True)

    args = parser.parse_args()

    catalog = args.catalog
    bronze_schema = args.bronze_schema
    silver_schema = args.silver_schema

    bronze_table = f"{catalog}.{bronze_schema}.orders_bronze"
    silver_table = f"{catalog}.{silver_schema}.orders_silver"

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    print(f"Reading from: {bronze_table}")
    print(f"Writing to: {silver_table}")

    # Read bronze table
    df = spark.table(bronze_table)

    # Transform according to actual dataset
    transformed_df = (
        df
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("amount", col("amount").cast("double"))
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
    )

    # Create table if not exists
    if not spark.catalog.tableExists(silver_table):
        print("Silver table does not exist. Creating new table.")
        (
            transformed_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )
    else:
        print("Silver table exists. Performing MERGE.")

        delta_table = DeltaTable.forName(spark, silver_table)

        (
            delta_table.alias("target")
            .merge(
                transformed_df.alias("source"),
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("Silver merge complete.")