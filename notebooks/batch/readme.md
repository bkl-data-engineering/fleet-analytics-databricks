# Batch Processing: Historical Transaction Pipeline
This module implements a full-scale historical ingestion and transformation pipeline for 50,000 vehicle transaction records. It follows a rigorous Medallion architecture to move data from raw logs to business-ready insights.

## Core Objective: 
To demonstrate a scalable batch ETL pattern that ensures data integrity through strict schema enforcement, deduplication, and partitioning.
1. **Bronze Layer: Raw Ingestion**
   * **Audit Trail:** Transactions are loaded from the source Volume as-is to preserve a permanent, immutable record of the original data.
   * **Metadata Enrichment:** Each record is tagged with ingestion_ts, ingestion_file_path, a unique run_id for lineage tracking and record_hash using a combination of columns for deduplication.

2. **Silver Layer: Cleaning & Optimization**
   This is the "Engine Room" of the pipeline where the data is refined and prepared for analytics.
   * **Schema Enforcement & Casting:** Utilized cast() to convert raw inputs into precise types (e.g., Double for financial metrics and Timestamp for events).
   * **Data Quality Filtering:** Implemented hard filters to remove invalid transactions where fuel_volume, fuel_price, or trip_distance were zero or negative, ensuring only valid operational data proceeds.
   * **Deduplication:** Utilized a record_hash (generated during ingestion) to drop duplicate records, ensuring each transaction is unique.
   * **Feature Engineering:** *Calculated fuel_cost ($Price \times Volume$) and miles_per_gallon ($Distance / Volume$).
        * Derived a trip_date column for optimized downstream storage.
   * **Performance Tuning:** The final Silver table is partitioned by trip_date, significantly reducing data scan costs for time-based reporting.
   
3. **Gold Layer: Analytical Aggregations**
   Utilizes the refined Silver data to serve three specific business-ready tables:
   * **Driver Performance:** Aggregates totals for fuel spend and distance, plus avg_mpg to rank driver efficiency.
   * **Vehicle Efficiency:** Summarizes fuel consumption at the asset level to assist in maintenance and fleet lifecycle decisions.
   * **Daily Fleet Summary:** Provides a high-level operational view, including daily transaction counts and total fleet mileage.

## Business Insights Demonstrated:
   * **Inefficiency Detection:** Includes logic to query the Gold layer for "Inefficient Drivers" whose average MPG falls below specific benchmarks.
   * **Consistency Checks:** Validates transaction densities to ensure historical data loads are complete (e.g., verifying dates with expected transaction counts).
   * **Data Governance:** Every table is professionally documented using Unity Catalog COMMENT ON TABLE syntax to ensure clarity for downstream analysts.
