# Databricks notebook source
# MAGIC %md
# MAGIC Enforce schema
# MAGIC Clean bad data
# MAGIC Deduplicate using record_hash
# MAGIC Add business metrics
# MAGIC Prepare for analytics
# MAGIC

# COMMAND ----------

raw_df = spark.read.format("delta").table("vehicle_transactions.bronze.vehicle_transactions_raw")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

# since this is vehicle txn data, we are using cast to enforce the schema. expectation is that if the data doesn't match the schema, the whole data will not be considered for silver layer.
raw_norm_df = raw_df.select(
    col("transaction_id").cast("string"),
    col("vehicle_id").cast("string"),
    col("driver_id").cast("string"),
    to_timestamp(col("timestamp")).alias("event_timestamp"),
    col("fuel_volume").cast("double"),
    col("fuel_price").cast("double"),
    col("trip_distance").cast("double"),
    col("odometer_reading").cast("double"),
    col("latitude").cast("double"),
    col("longitude").cast("double"),
    col("record_hash"),
    col("ingestion_file_path"),
    col("run_id"),
    col("ingestion_ts")
)

# COMMAND ----------

silver_vt_df = raw_norm_df.filter(
    (col("fuel_volume") > 0) & 
    (col("fuel_price") > 0) & 
    (col("trip_distance") > 0)
)

# COMMAND ----------

silver_vt_df = silver_vt_df.dropDuplicates(["record_hash"])

# COMMAND ----------

# add derived columns fuel cost and mpg
silver_vt_df = silver_vt_df.withColumn("fuel_cost", col("fuel_volume") * col("fuel_price"))
silver_vt_df = silver_vt_df.withColumn("miles_per_gallon", col("trip_distance") / col("fuel_volume"))

# COMMAND ----------

# let us partition by a trip date column. to do that let's introduce a new column trip_date
from pyspark.sql.functions import to_date, col

silver_vt_df = silver_vt_df.withColumn("trip_date", to_date(col("event_timestamp")))



# COMMAND ----------

display(silver_vt_df.columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists vehicle_transactions.silver;

# COMMAND ----------

# store the partitioned data in a new table in the silver schema
silver_vt_df.write.partitionBy("trip_date").mode("overwrite").format("delta").saveAsTable("vehicle_transactions.silver.fleet_transactions_processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.silver.fleet_transactions_processed limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.silver.fleet_transactions_processed
# MAGIC where trip_date = to_date('2024-01-01') limit 10;