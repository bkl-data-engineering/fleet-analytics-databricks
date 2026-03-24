# fleet-analytics-databricks

# Fleet Analytics Data Platform (Databricks)

##  Overview
Built an end-to-end data engineering pipeline using Databricks to analyze fleet fuel transactions and driver behavior.

##  Architecture
- Bronze: Raw ingestion with run tracking and metadata
- Silver: Data cleaning, schema enforcement, deduplication
- Gold: Aggregated business insights
- Data Quality: Validation checks with run-level monitoring

##  Tech Stack
- Databricks
- PySpark
- Delta Lake
- SQL

##  Key Features
- Medallion architecture (Bronze → Silver → Gold)
- Run-level lineage using `run_id`
- Deduplication using `record_hash`
- Data quality framework (null, range, duplicate, outlier checks)
- Partitioned data for performance optimization

##  Sample Insights
- Driver fuel spend analysis
- Vehicle efficiency metrics (MPG)
- Daily fleet trends

##  Data Quality Monitoring
- Run-level validation tracking
- Historical DQ metrics stored in Delta tables
- Basic alerting for failed checks

##  Project Structure
(mention folders)

##  Future Improvements
- Streaming ingestion (Kafka / Spark Streaming)
- AI-based anomaly detection
- Dashboard integration (Power BI / Tableau)
