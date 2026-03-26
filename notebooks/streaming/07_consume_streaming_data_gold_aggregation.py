# Databricks notebook source
# MAGIC %sql
# MAGIC create volume if not exists vehicle_transactions.gold.checkpoints;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum
driver_spend_df = (
    spark.readStream.table("vehicle_transactions.silver.fleet_txn_fs_stream_processed").groupBy("driver_id")
    .agg(
        sum("fuel_cost").alias("total_spend")
    )
)

# COMMAND ----------


def driver_spend_summary_upsert(stream_df, batch_id):
    row_count = stream_df.count()
    print(f"Processing Batch ID: {batch_id}, Row Count: {row_count}")
    
    stream_df.createOrReplaceTempView("driver_spend_summary")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS vehicle_transactions.gold.fs_driver_spend_summary (
            driver_id STRING,
            total_spend DOUBLE,
            PRIMARY KEY(driver_id)
        )
        USING DELTA
    """)

    spark.sql("""
        MERGE INTO vehicle_transactions.gold.fs_driver_spend_summary target
        USING driver_spend_summary source
        ON target.driver_id = source.driver_id
        WHEN MATCHED THEN
            UPDATE SET target.total_spend = target.total_spend + source.total_spend
        WHEN NOT MATCHED
            THEN INSERT (driver_id, total_spend) VALUES (source.driver_id, source.total_spend)
    """)

# COMMAND ----------


checkpoint_path = "/Volumes/vehicle_transactions/gold/checkpoints/fleet_txn_fs_stream_chk_path"
driver_agg_query = driver_spend_df.writeStream \
    .foreachBatch(driver_spend_summary_upsert) \
    .trigger(availableNow=True) \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .outputMode("update") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.gold.fs_driver_spend_summary;

# COMMAND ----------

