# Databricks notebook source
# MAGIC %md
# MAGIC Using the vehicle transactions data which has been processed through the raw bronze layer and enriched silver layer, this notebook captures building aggregations of data useful to provide Driver and spend analysis.
# MAGIC Metrics that will be served:
# MAGIC - Driver Performance
# MAGIC - Vehicle Efficiency
# MAGIC - Daily Fleet Summary
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists vehicle_transactions.gold;

# COMMAND ----------

df_silver = spark.table("vehicle_transactions.silver.fleet_transactions_processed")

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, round

df_driver_perf = df_silver.groupBy("driver_id").agg (
    round(sum("fuel_cost"),3).alias("total_fuel_cost"),
    round(sum("trip_distance"),3).alias("total_distance_driven"),
    round(avg("miles_per_gallon"),3).alias("avg_mpg")                                    
)
df_driver_perf.write.format("delta").mode("overwrite").saveAsTable("vehicle_transactions.gold.driver_performance")

# add comment for driver_performance table
spark.sql("""
COMMENT ON TABLE vehicle_transactions.gold.driver_performance IS 'This table contains the total fuel cost, total distance driven, and average MPG for each driver.'
""")

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, round
# build dataframe for capturing vehicle efficience
df_veh_eff = df_silver.groupBy("vehicle_id").agg (
    round(sum("fuel_volume"), 3).alias("total_fuel_used"),
    round(sum("trip_distance"), 3).alias("total_distance_driven"),
    round(avg("miles_per_gallon"), 3).alias("avg_mpg")
)

#save it to the vehicle_efficiency table
df_veh_eff.write.format("delta").mode("overwrite").saveAsTable("vehicle_transactions.gold.vehicle_efficiency")

# add comment for vehicle_efficiency table
spark.sql("""
COMMENT ON TABLE vehicle_transactions.gold.vehicle_efficiency IS 'This table contains the total fuel used, total distance driven, and average MPG for each vehicle. The total_fuel_used is a sum of fuel_volume across all the rows and the trip_distance is the sum of trip_distance across all the rows. the avg_mpg is an average taken on the miles_per_gallon'
""")

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, round, count
# build a new table for daily fleet summary. This table will have data aggregated by trip_date column and will show the number of transactions along with the sum(fuel_cost), sum(trip_distance))
df_daily_fleet_summ = df_silver.groupBy("trip_date").agg (
    round(sum("fuel_cost"), 3).alias("total_fuel_cost"),
    round(sum("trip_distance"), 3).alias("total_distance_driven"),
    round(count("transaction_id"), 3).alias("total_transactions")
)

#save it to the daily_fleet_summary table)
df_daily_fleet_summ.write.format("delta").mode("overwrite").saveAsTable("vehicle_transactions.gold.daily_fleet_summary")

# add comment for daily_fleet_summary table
spark.sql("""
COMMENT ON TABLE vehicle_transactions.gold.daily_fleet_summary IS 'This table contains the total fuel cost, total distance driven, and total transactions for each day. The data is aggregated by trip_date and shows the total fuel spend and total distance driven for each day as well as the number of fuel transactions.'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.gold.daily_fleet_summary
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vehicle_transactions.gold.daily_fleet_summary
# MAGIC where total_transactions between 300 and 310
# MAGIC order by trip_date;

# COMMAND ----------

#identify inefficient_drivers
df_inefficient_drivers = spark.sql("select * from vehicle_transactions.gold.driver_performance where avg_mpg <= 10")

df_inefficient_drivers.show()