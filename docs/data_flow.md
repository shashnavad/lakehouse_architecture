# Data Flow Documentation

This document describes the detailed data flow through the Lakehouse Architecture pipeline.

## Overview

The data flows through three main layers: Bronze → Silver → Gold, with each layer performing specific transformations and validations.

## Bronze Layer Flow

### Input Sources
- CSV files (transactions, customers, products)
- JSON files (optional)
- Parquet files (optional)
- Kafka streams (streaming mode)

### Processing Steps

1. **Read Raw Data**
   - Read from source files/streams
   - Apply schema (strict or flexible)
   - Handle encoding issues

2. **Add Metadata**
   - `ingestion_timestamp`: When data was ingested
   - `source_file`: Original file path
   - `data_source`: Source system identifier
   - `ingestion_date`: Date partition for efficient querying

3. **Write to Delta Lake**
   - Append mode (immutable)
   - Schema enforcement
   - Partition by ingestion_date

### Output
- Delta tables in `bronze/` path
- Tables: `bronze.transactions`, `bronze.customers`, `bronze.products`

## Silver Layer Flow

### Input
- Delta tables from Bronze layer

### Processing Steps

1. **Read from Bronze**
   - Read latest version of Bronze tables
   - Filter by ingestion_date for incremental processing

2. **Data Quality Checks**
   - **Null Handling**: Identify and handle null values
   - **Duplicate Detection**: Remove duplicate records
   - **Type Validation**: Ensure data types match schema
   - **Range Validation**: Check value ranges (e.g., prices > 0)
   - **Referential Integrity**: Validate foreign keys

3. **Data Transformation**
   - Column renaming and standardization
   - Data type conversions
   - Derived columns (e.g., profit = price - cost)
   - Data enrichment (join with reference data)

4. **Quarantine Handling**
   - Records failing validation → `silver.quarantine`
   - Log validation errors
   - Maintain audit trail

5. **Write to Silver**
   - Upsert mode (MERGE operation)
   - Deduplication based on business keys
   - Incremental processing (only new/changed records)

### Output
- Clean Delta tables in `silver/` path
- Tables: `silver.transactions`, `silver.customers`, `silver.products`
- Quarantine table: `silver.quarantine`

## Gold Layer Flow

### Input
- Clean Delta tables from Silver layer

### Processing Steps

1. **Read from Silver**
   - Read cleaned data from Silver tables
   - Apply business filters if needed

2. **Business Aggregations**

   **Daily Sales Summary**:
   - Total revenue by day
   - Transaction count
   - Average order value
   - Top products
   - Top customers

   **Customer Segments**:
   - Customer lifetime value
   - Purchase frequency
   - Recency analysis
   - Segment classification

   **Product Performance**:
   - Sales by product
   - Revenue by category
   - Profit margins
   - Stock turnover

3. **Dimension Tables**
   - `dim_customers`: Customer master data
   - `dim_products`: Product master data
   - `dim_dates`: Date dimension (for time-based analysis)

4. **Optimization**
   - Partition by date/category
   - Z-order by frequently queried columns
   - OPTIMIZE command for file compaction

### Output
- Aggregated Delta tables in `gold/` path
- Tables: `gold.daily_sales_summary`, `gold.customer_segments`, etc.
- Dimension tables: `gold.dim_*`

## Incremental Processing

### Strategy
- Process only new records since last run
- Track last processed timestamp
- Use Delta Lake change data feed (if enabled)

### Implementation
```python
# Read only new records
last_processed = get_last_processed_timestamp()
new_data = bronze_df.filter(col("ingestion_timestamp") > last_processed)
```

## Streaming Flow

### Kafka → Bronze
1. Spark Structured Streaming reads from Kafka
2. Apply watermark for late-arriving data
3. Write micro-batches to Delta Lake
4. Exactly-once semantics

### Bronze → Silver (Streaming)
1. Continuous processing of new Bronze records
2. Apply same quality checks as batch
3. Stream-to-stream joins if needed

### Silver → Gold (Streaming)
1. Incremental aggregations
2. Update existing records (MERGE)
3. Maintain state for windowed aggregations

## Error Handling

### Bronze Layer
- Schema mismatches → Log and skip or use quarantine
- File read errors → Retry with exponential backoff

### Silver Layer
- Validation failures → Quarantine table
- Transformation errors → Log and alert
- Duplicate keys → Use latest record (configurable)

### Gold Layer
- Aggregation errors → Log and continue with partial results
- Join failures → Use outer joins or default values

## Data Lineage

```
Source Files
    ↓
Bronze Layer (Raw + Metadata)
    ↓
Silver Layer (Cleaned + Enriched)
    ↓
Gold Layer (Aggregated + Optimized)
    ↓
Analytics & Reporting
```

## Performance Optimizations

1. **Partitioning**: Partition by date for time-series queries
2. **Z-Ordering**: Co-locate related data for faster scans
3. **File Sizing**: OPTIMIZE to maintain 128MB-1GB file sizes
4. **Caching**: Cache frequently accessed dimension tables
5. **Incremental Processing**: Only process new/changed data

## Monitoring Points

- **Bronze**: Records ingested, ingestion latency, schema errors
- **Silver**: Records processed, validation failures, quarantine count
- **Gold**: Aggregation latency, query performance, table sizes

