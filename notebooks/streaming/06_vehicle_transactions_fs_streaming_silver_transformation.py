# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists vehicle_transactions.silver;
# MAGIC create volume if not exists vehicle_transactions.silver.checkpoints;

# COMMAND ----------

raw_stream_df = spark.readStream.table("vehicle_transactions.bronze.vehicle_transactions_fs_stream_raw")

# COMMAND ----------

# set appropriate datatypes for the columns.
from pyspark.sql.functions import col, to_timestamp
enriched_stream_df = (
    raw_stream_df
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    .withColumn("fuel_volume", col("fuel_volume").cast("double")) 
    .withColumn("fuel_price", col("fuel_price").cast("double")) 
    .withColumn("trip_distance", col("trip_distance").cast("double")) 
    .withColumn("odometer_reading", col("odometer_reading").cast("double")) 
    .withColumn("latitude", col("latitude").cast("double")) 
    .withColumn("longitude", col("longitude").cast("double"))
)

# COMMAND ----------


enriched_stream_df = (enriched_stream_df
                      .filter(
                        (col("fuel_price") > 0) &
                        (col("fuel_volume") > 0) & 
                        (col("trip_distance") > 0)
                        )
                      .withColumn("fuel_cost", (col("fuel_price") * col("fuel_volume")).cast("double"))
                      .withColumn("miles_per_gallon", (col("trip_distance") / col("fuel_volume")).cast("double"))
                      )

# COMMAND ----------


enriched_stream_df = (
    enriched_stream_df
    .withWatermark("event_timestamp", "1 day")
    .dropDuplicates(["record_hash"])
)

# COMMAND ----------

checkpoint_path = "/Volumes/vehicle_transactions/silver/checkpoints/vehicle_txn_stream_enriched/"
silver_tfm_query = (enriched_stream_df.writeStream \
    .trigger(availableNow=True)
    .format("delta") 
    .option("checkpointLocation", checkpoint_path) 
    .outputMode("append") 
    .table("vehicle_transactions.silver.fleet_txn_fs_stream_processed")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.silver.fleet_txn_fs_stream_processed
# MAGIC order by ingestion_ts desc limit 50;

# COMMAND ----------

