# Setup Guide

This guide will help you set up the Lakehouse Architecture project from scratch.

## Prerequisites

Before starting, ensure you have the following installed:

- **Docker** (version 20.10+) and **Docker Compose** (version 1.29+)
- **Python** 3.8 or higher
- **Java** 8 or 11 (required for Spark)
- **Git** (for version control)

### Verify Prerequisites

```bash
# Check Docker
docker --version
docker-compose --version

# Check Python
python3 --version

# Check Java
java -version

# Check Git
git --version
```

## Step 1: Clone and Navigate

```bash
git clone <repository-url>
cd lakehouse_architecture
```

## Step 2: Set Up Python Environment

Create a virtual environment and install dependencies:

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Step 3: Start Infrastructure Services

Start all required services using Docker Compose:

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### Services and Ports

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MinIO API**: http://localhost:9000
- **Jupyter Notebook**: http://localhost:8888
- **Airflow**: http://localhost:8080 (admin/admin)
- **Kafka**: localhost:9092
- **PostgreSQL**: localhost:5432

## Step 4: Generate Sample Data

Generate synthetic e-commerce data:

```bash
python scripts/data_generator.py
```

This will create:
- `data/raw/customers.csv`
- `data/raw/products.csv`
- `data/raw/transactions.csv`

## Step 5: Configure Storage

### Option A: Use MinIO (S3-compatible)

1. Access MinIO Console: http://localhost:9001
2. Login with: `minioadmin` / `minioadmin`
3. Create a bucket named `lakehouse`
4. Upload raw data files to `lakehouse/raw/`

### Option B: Use Local Filesystem

The pipeline will automatically use local filesystem if MinIO is not configured.

Set environment variable:
```bash
export USE_MINIO=false
```

## Step 6: Run Bronze Layer Ingestion

```bash
python scripts/bronze/ingest_raw_data.py
```

This will:
- Read raw CSV files
- Add metadata columns (ingestion timestamp, source file, etc.)
- Write to Delta Lake Bronze tables

## Step 7: Verify Setup

### Check MinIO (if using)

```bash
# List buckets
docker exec lakehouse_minio mc ls minio/
```

### Check Spark Tables

```python
from config.spark_config import create_spark_session
spark, base_path = create_spark_session()
spark.sql("SHOW TABLES").show()
```

## Troubleshooting

### Docker Issues

```bash
# Restart services
docker-compose restart

# Rebuild containers
docker-compose up -d --build

# Clean up and start fresh
docker-compose down -v
docker-compose up -d
```

### Python/Spark Issues

```bash
# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Check Java version
java -version  # Should be 8 or 11
```

### Port Conflicts

If ports are already in use, modify `docker-compose.yml` to use different ports.

## Next Steps

Once setup is complete, proceed to:
1. **Phase 2**: Bronze Layer Ingestion ✓
2. **Phase 3**: Silver Layer Processing
3. **Phase 4**: Gold Layer Aggregations

See [Architecture Documentation](architecture.md) for more details.

