# Streaming & Incremental Ingestion: Fleet Transaction Analytics
This module implements a high-performance, incremental pipeline using Spark Structured Streaming and Databricks Auto Loader. It processes vehicle transaction logs in real-time (simulated with 100-300 row micro-batches) to provide up-to-the-minute fleet insights.

## Core Objective: 
To demonstrate a cost-efficient, fault-tolerant ingestion pattern that handles Schema Evolution and Stateful Upserts without manual intervention.
1. **Advanced Ingestion:** Auto Loader (cloudFiles)Unlike the Batch process which requires manual schema definition and full-table scans, the Streaming layer uses Auto Loader for a "set and forget" ingestion pattern:
   * **Schema Inference & Evolution:** Auto Loader automatically detects the transaction schema. I configured it to handle Schema Evolution, allowing the pipeline to adapt if new fields (like fuel_type or station_id) are added to the source CSVs in the future.
   * **The Checkpoint Mechanism:** Every micro-batch is tracked in a checkpointLocation. This ensures Exactly-Once Processing semantics—if the cluster restarts, the stream resumes precisely where it left off, preventing duplicate transaction records.
   * **Optimized Triggering:** Used trigger(availableNow=True) to process all available transaction files as a single incremental batch. This provides the benefits of streaming logic with the cost-profile of a batch job.

2. **Incremental Silver Transformations**
   The Silver layer processes data "in-flight" using the same rigorous logic as the Batch pipeline but applied to micro-batches:
   * **Streaming Deduplication:** Records are deduplicated against the record_hash within the current stream state.
   * **Dynamic Feature Engineering:** Calculations for fuel_cost and miles_per_gallon are performed as data moves through the pipe, ensuring the Silver table is always "ready for consumption.
   
3. **Stateful Gold Aggregation** (The foreachBatch Pattern)
   The highlight of this module is the transition from "Overwrite" logic to "Upsert" (Merge) logic.
   * **Stateful Updates:** While the Batch process rewrites the Gold tables, the Streaming process uses a Delta MERGE inside a foreachBatch sink.
   * **Why this matters:** When a driver completes a new transaction, the pipeline only updates that specific driver's record in the driver_performance table. It adds the new fuel_cost to the existing total, rather than recalculating the entire fleet's history.
   * **Scalability:** This pattern allows the system to handle millions of drivers while keeping update times to mere seconds.

4. **Technical Comparison: Batch vs. Streaming**
   
  | Feature | Batch Ingestion | Streaming (Auto Loader) |
  | :--- | :--- | :--- |
  | **Data Discovery** | Full Directory Scan | **Incremental (`cloudFiles`)** |
  | **Schema Handling** | Manual / Static | **Auto-Inference & Evolution** |
  | **Processing Trigger** | Manual / Scheduled | **Trigger Once / AvailableNow** |
  | **Gold Write Mode** | `overwrite` | **`merge` (Stateful Upsert)** |
  | **Compute Efficiency** | Re-processes all data | **Processes only new data** |
  | **Latency** | High (Batch Windows) | **Low (Incremental Batches)** |
    
