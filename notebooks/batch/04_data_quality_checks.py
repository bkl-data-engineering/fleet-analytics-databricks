# Databricks notebook source
# MAGIC %md
# MAGIC check the data hygiene and quality
# MAGIC - null checks
# MAGIC - range checks
# MAGIC - duplicate checks
# MAGIC - outlier detection
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, count

df_silver = spark.table("vehicle_transactions.silver.fleet_transactions_processed")

#1 null data check
df_null_check = df_silver.filter(
    (col("transaction_id").isNull()) |
    (col("vehicle_id").isNull()) |
    (col("driver_id").isNull()) |
    (col("event_timestamp").isNull())
)

#2 range check
df_range_check = df_silver.filter(
    (col("fuel_volume") < 0) |
    (col("fuel_price") < 0) |
    (col("trip_distance") < 0) 
)

#3 duplicate data check
df_dup_check = df_silver.groupBy("record_hash") \
    .agg(count("*").alias ("dup_count")) \
    .filter(col("dup_count") > 1)

#4 outlier detection
df_outlier = df_silver.filter(
    ((col("miles_per_gallon") < 0) | (col("miles_per_gallon") > 100))
)


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime

# Use standard Python datetime for a Python list, not Spark's current_timestamp()
dq_check_ts = datetime.now()
current_run_id = df_silver.orderBy(col("ingestion_ts").desc()).select("run_id").first()[0]

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("dq_timestamp", TimestampType(), True),
    StructField("dq_check", StringType(), True),
    StructField("failed_count", LongType(), True)
])

dq_summary_lst = [
    Row(run_id=current_run_id, dq_timestamp=dq_check_ts, dq_check="Null Data Check", failed_count=df_null_check.count()),
    Row(run_id=current_run_id, dq_timestamp=dq_check_ts, dq_check="Range Check", failed_count=df_range_check.count()),
    Row(run_id=current_run_id, dq_timestamp=dq_check_ts, dq_check="Duplicate Data Check", failed_count=df_dup_check.count()),
    Row(run_id=current_run_id, dq_timestamp=dq_check_ts, dq_check="Outlier Detection", failed_count=df_outlier.count())
]

df_dq_summary = spark.createDataFrame(dq_summary_lst, schema=schema)
df_dq_summary.write.format("delta").mode("append").saveAsTable("vehicle_transactions.silver.data_quality_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, dq_check, failed_count, dq_timestamp
# MAGIC FROM vehicle_transactions.silver.data_quality_summary
# MAGIC ORDER BY dq_timestamp DESC;
# MAGIC

# COMMAND ----------



# COMMAND ----------

