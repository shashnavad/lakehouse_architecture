# Lakehouse Architecture Project

A comprehensive implementation of a modern data lakehouse architecture using Delta Lake on MinIO (S3-compatible storage) - **100% Free Tier**.

## 🏗️ Architecture Overview

This project implements the **Medallion Architecture** (Bronze → Silver → Gold) with Delta Lake, providing ACID transactions, time travel, and schema evolution capabilities.

```
┌─────────────┐
│  Raw Data   │ (CSV/JSON/Parquet)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Bronze Layer│ (Raw ingestion with metadata)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Silver Layer│ (Cleaned & Enriched)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Gold Layer │ (Business Aggregates)
└─────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Java 8 or 11 (for Spark)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd lakehouse_architecture
   ```

2. **Start infrastructure services**
   ```bash
   docker-compose up -d
   ```

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the pipeline**
   ```bash
   # Complete pipeline (Bronze → Silver → Gold)
   python scripts/run_pipeline.py
   
   # Or use Makefile commands
   make run-pipeline
   ```

5. **Explore Delta Lake features** (optional)
   ```bash
   make delta-features
   ```

## 📁 Project Structure

```
lakehouse_architecture/
├── docker-compose.yml          # Infrastructure services (MinIO, Jupyter, etc.)
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── docs/                       # Documentation
│   ├── architecture.md        # Detailed architecture docs
│   ├── setup.md               # Setup instructions
│   └── data_flow.md           # Data flow documentation
├── notebooks/                  # Jupyter notebooks
│   └── demo.ipynb             # Demo notebook
├── scripts/                    # Python scripts
│   ├── bronze/                # Bronze layer pipelines
│   ├── silver/                # Silver layer pipelines
│   ├── gold/                  # Gold layer pipelines
│   ├── delta_features/        # Delta Lake advanced features
│   └── data_generator.py      # Synthetic data generator
├── config/                     # Configuration files
│   └── spark_config.py        # Spark configuration
└── tests/                      # Unit tests
```

## 🛠️ Tech Stack

- **Core:** Python, PySpark, Delta Lake
- **Storage:** MinIO (S3-compatible) or Local Filesystem
- **Orchestration:** Apache Airflow (Docker)
- **Streaming:** Apache Kafka, Spark Structured Streaming
- **Data Quality:** Great Expectations
- **Monitoring:** Streamlit Dashboard
- **Infrastructure:** Docker, Docker Compose

## 📊 Features

### ✅ Implemented (Phases 1-5)
- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Bronze Layer: Raw data ingestion with metadata
- ✅ Silver Layer: Data quality checks, transformations, MERGE operations
- ✅ Gold Layer: Business aggregations, dimension tables, partitioning
- ✅ Data Quality Framework (null checks, duplicates, referential integrity)
- ✅ Incremental Processing
- ✅ **Delta Lake Advanced Features:**
  - ✅ Time Travel (query by version/timestamp, RESTORE)
  - ✅ ACID Transactions (atomicity, consistency, isolation, durability)
  - ✅ Schema Evolution (add columns without breaking queries)
  - ✅ OPTIMIZE (file compaction, Z-Ordering)
  - ✅ VACUUM (cleanup with retention policy)
- ✅ Comprehensive Test Suite (pytest)

### 🚧 In Progress (Phases 6-10)
- ⏳ Streaming Data Processing (Kafka, Spark Structured Streaming)
- ⏳ Orchestration (Airflow DAGs)
- ⏳ Monitoring Dashboard (Streamlit/Grafana)

## 📝 Documentation

See the [docs/](docs/) directory for detailed documentation:
- [Architecture Details](docs/architecture.md)
- [Setup Guide](docs/setup.md)
- [Data Flow](docs/data_flow.md)
- [Delta Lake Features](docs/delta_lake_features.md)

## 🤝 Contributing

This is a learning project. Feel free to fork and experiment!

## 📄 License

MIT License

## 👤 Author

**shashnavad**

---

*Built with ❤️ using free-tier technologies*

