# Databricks notebook source
# MAGIC %sql
# MAGIC create volume if not exists vehicle_transactions.bronze.fleet_txn_fs_stream;
# MAGIC create volume if not exists vehicle_transactions.bronze.checkpoints;

# COMMAND ----------


source_path = "/Volumes/vehicle_transactions/bronze/fleet_txn_fs_stream"
checkpoint_path = "/Volumes/vehicle_transactions/bronze/checkpoints/vehicle_txn_fs_stream/"
schema_path = "/Volumes/vehicle_transactions/bronze/checkpoints/vehicle_txn_schema_metadata/"

ft_stream_df = (spark.readStream
                   .format("cloudFiles")
                   .option("cloudFiles.format", "csv")
                   .option("cloudFiles.schemaLocation", schema_path)
                   .option("inferColumnTypes", "true")
                   .option("header", "true")
                   .load(source_path))

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat_ws, sha2
ft_stream_df = ft_stream_df.withColumn("ingestion_ts", current_timestamp()) \
                          .withColumn("ingestion_stream_path", col("_metadata.file_path")) \
                          .withColumn("record_hash", sha2(concat_ws("||", "transaction_id", "vehicle_id", "timestamp"), 256))

# COMMAND ----------

ft_stream_df.writeStream \
    .trigger(availableNow=True) \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .table("vehicle_transactions.bronze.vehicle_transactions_fs_stream_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vehicle_transactions.bronze.vehicle_transactions_fs_stream_raw;

# COMMAND ----------

