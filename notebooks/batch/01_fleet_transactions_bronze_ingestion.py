# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists vehicle_transactions;
# MAGIC create schema if not exists vehicle_transactions.bronze;
# MAGIC create volume if not exists vehicle_transactions.bronze.raw_data;

# COMMAND ----------

raw_df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load('/Volumes/vehicle_transactions/bronze/raw_data/fleet_transactions.csv') \
    .select("*", "_metaData.file_path")

# COMMAND ----------

from pyspark.sql.functions import lit, concat_ws, current_timestamp, sha2, col
import uuid

run_id = str(uuid.uuid4())

exclude_cols_list = ["file_path"]
columns_to_hash = [col(c) for c in raw_df.columns if c not in exclude_cols_list]

raw_df = raw_df.withColumn("record_hash", sha2(concat_ws("||", *columns_to_hash), 256)) \
                .withColumn ("run_id", lit(run_id)) \
                .withColumn("ingestion_ts", current_timestamp()) \
                .withColumnRenamed("file_path", "ingestion_file_path")

# COMMAND ----------

raw_df.write.format('delta').mode('overwrite').saveAsTable("vehicle_transactions.bronze.vehicle_transactions_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.bronze.vehicle_transactions_raw limit 5;

# COMMAND ----------

print(raw_df.columns)

# COMMAND ----------

raw_df.createOrReplaceTempView("vehicle_txn_raw")
raw_df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_txn_raw limit 5;

# COMMAND ----------

