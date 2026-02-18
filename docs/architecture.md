# Architecture Documentation

## Overview

This project implements a modern **Lakehouse Architecture** using the Medallion (Bronze-Silver-Gold) pattern with Delta Lake, providing ACID transactions, time travel, and schema evolution.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │   CSV    │  │   JSON   │  │ Parquet  │  │  Kafka   │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼─────────────┼──────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │      BRONZE LAYER (Raw Data)         │
        │  • Raw ingestion with metadata        │
        │  • Schema enforcement                │
        │  • Ingestion timestamps              │
        │  • Source tracking                   │
        └──────────────────┬───────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │      SILVER LAYER (Cleaned Data)      │
        │  • Data quality checks                │
        │  • Deduplication                      │
        │  • Type validation                    │
        │  • Standardization                    │
        │  • Quarantine for failed records      │
        └──────────────────┬───────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────┐
        │      GOLD LAYER (Business Data)      │
        │  • Business aggregations              │
        │  • Dimension tables                   │
        │  • Optimized partitioning             │
        │  • Ready for analytics                │
        └──────────────────────────────────────┘
```

## Technology Stack

### Core Technologies

- **Apache Spark 3.5+**: Distributed data processing engine
- **Delta Lake 2.4+**: ACID transactions and time travel on data lakes
- **PySpark**: Python API for Spark

### Storage

- **MinIO**: S3-compatible object storage (free tier)
- **Local Filesystem**: Alternative for local development

### Orchestration

- **Apache Airflow**: Workflow orchestration and scheduling
- **Docker Compose**: Container orchestration

### Streaming (Optional)

- **Apache Kafka**: Distributed streaming platform
- **Spark Structured Streaming**: Real-time data processing

### Data Quality

- **Great Expectations**: Data validation framework
- **Custom Validation**: Python-based quality checks

### Monitoring

- **Streamlit**: Interactive dashboard
- **Grafana**: Metrics visualization (optional)

## Layer Details

### Bronze Layer

**Purpose**: Store raw, unprocessed data exactly as received from source systems.

**Characteristics**:
- Append-only (immutable)
- Schema enforcement
- Metadata columns (ingestion_timestamp, source_file, data_source)
- No data transformation
- Preserves original data for audit and reprocessing

**Tables**:
- `bronze.transactions`
- `bronze.customers`
- `bronze.products`

### Silver Layer

**Purpose**: Cleaned, validated, and enriched data ready for analytics.

**Characteristics**:
- Data quality checks (nulls, duplicates, type validation)
- Standardized formats
- Deduplication
- Enriched with additional context
- Quarantine table for failed records
- Incremental processing

**Tables**:
- `silver.transactions`
- `silver.customers`
- `silver.products`
- `silver.quarantine` (failed records)

### Gold Layer

**Purpose**: Business-level aggregated data optimized for reporting and analytics.

**Characteristics**:
- Business metrics and KPIs
- Pre-aggregated summaries (daily, weekly, monthly)
- Dimension tables (star schema)
- Optimized partitioning
- Fast query performance

**Tables**:
- `gold.daily_sales_summary`
- `gold.customer_segments`
- `gold.product_performance`
- `gold.dim_customers`
- `gold.dim_products`

## Delta Lake Features

### ACID Transactions
- Ensures data consistency
- Supports concurrent reads and writes
- Rollback capability

### Time Travel
- Query historical versions of data
- Audit trail
- Point-in-time recovery

### Schema Evolution
- Add new columns without breaking existing queries
- Automatic schema merging
- Schema enforcement options

### Optimizations
- **OPTIMIZE**: Compact small files for better performance
- **VACUUM**: Clean up old versions (retention policy)
- **Z-Ordering**: Co-locate related data

## Data Flow

### Batch Processing Flow

1. **Data Generation**: Synthetic data generator creates sample data
2. **Bronze Ingestion**: Raw data ingested with metadata
3. **Silver Processing**: Data cleaned and validated
4. **Gold Aggregation**: Business metrics calculated
5. **Monitoring**: Metrics logged and visualized

### Streaming Flow

1. **Kafka Producer**: Generates real-time events
2. **Spark Streaming**: Consumes from Kafka
3. **Bronze Streaming**: Writes to Delta Lake with watermarking
4. **Incremental Silver**: Processes new records only
5. **Incremental Gold**: Updates aggregations incrementally

## Scalability

- **Horizontal Scaling**: Spark distributes processing across nodes
- **Partitioning**: Data partitioned by date/category for efficient queries
- **File Optimization**: Delta Lake OPTIMIZE command for performance
- **Incremental Processing**: Only process new/changed data

## Security

- **Access Control**: MinIO bucket policies (if using MinIO)
- **Data Encryption**: At-rest and in-transit (configurable)
- **Audit Logging**: All operations logged with timestamps

## Cost Optimization

- **Free Tier**: All components use free/open-source versions
- **Local Storage**: Option to use local filesystem instead of cloud
- **Resource Management**: Spark resource allocation tuning
- **Data Retention**: VACUUM for old data cleanup

## Performance Considerations

- **Partitioning Strategy**: Partition by date for time-series data
- **Z-Ordering**: Co-locate frequently queried columns
- **File Sizing**: OPTIMIZE to maintain optimal file sizes
- **Caching**: Spark caching for frequently accessed data

## Monitoring & Observability

- **Pipeline Metrics**: Rows processed, latency, error rates
- **Spark Metrics**: Execution time, shuffle size, memory usage
- **Delta Lake Stats**: File count, table size, version history
- **Dashboard**: Real-time visualization of metrics

## Next Steps

- See [Setup Guide](setup.md) for installation instructions
- See [Data Flow](data_flow.md) for detailed pipeline documentation

