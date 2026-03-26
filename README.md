# 🚚 Fleet Transaction Analytics: End-to-End Medallion Pipeline

## 📌 Project Overview
This project demonstrates a production-grade Medallion Architecture using Databricks Serverless and Delta Lake. I implemented two distinct data movement patterns—Batch and Incremental Streaming—to process vehicle transaction records (fueling events, odometer readings, and cost data).

The pipeline transforms raw transactional logs into a high-value Driver Spend Summary, calculating fuel efficiency and operational costs across a fleet of vehicles.

## 🏗️ Architecture & Technical Stack
* Platform: Databricks (Serverless Compute)
* Storage: Unity Catalog (Managed Volumes & Tables)
* Engine: PySpark / Spark Structured Streaming
* Ingestion: Auto Loader (cloudFiles)
* Format: Delta Lake (with Schema Evolution & Enforcement)



## 🚀 Key Engineering Highlights
1. Unified Ingestion with Auto Loader
Implemented Auto Loader to handle file-based streaming of transaction logs from Unity Catalog Volumes.
Idempotent Processing: Utilizes Checkpoints to ensure that each transaction file is processed exactly once, ensuring financial accuracy.
Cost Optimization: Used the AvailableNow trigger to process pending data as an incremental batch, maximizing Serverless efficiency.

2. Silver Layer Transformation & Enrichment
Transformed raw transaction strings into a typed schema with business-ready logic:
Financial Calculations: Calculated fuel_cost ($fuel\_price \times fuel\_volume$) during the Silver stream.
Data Cleaning: Enforced data types for odometer_reading and trip_distance to ensure downstream reporting accuracy.

3. Stateful Gold Layer (The CDC Pattern)
Implemented a Change Data Capture (CDC) pattern for the final aggregation:
foreachBatch Upserts: Used foreachBatch to run a Delta MERGE operation, updating driver totals without rewriting the entire table.
Efficiency: Only rows for drivers with new transactions are updated, making the pipeline highly scalable for large fleets.




