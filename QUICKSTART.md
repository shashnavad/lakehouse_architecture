# Quick Start Guide

Get your Lakehouse Architecture up and running in 5 minutes!

## Prerequisites Check

```bash
# Verify Docker is installed
docker --version
docker-compose --version

# Verify Python 3.8+
python3 --version

# Verify Java 8 or 11
java -version
```

## Step 1: Setup Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Step 2: Start Infrastructure

```bash
# Start all Docker services (MinIO, Jupyter, Airflow, Kafka)
make start

# Or manually:
docker-compose up -d
```

**Wait 30-60 seconds** for services to initialize.

## Step 3: Verify Services

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Jupyter**: http://localhost:8888
- **Airflow**: http://localhost:8080 (admin/admin)

## Step 4: Generate Sample Data

```bash
# Generate e-commerce transaction data
make generate-data

# This creates:
# - data/raw/customers.csv
# - data/raw/products.csv
# - data/raw/transactions.csv
```

## Step 5: Run Bronze Ingestion

```bash
# Option 1: Use local filesystem (default)
export USE_MINIO=false
python scripts/bronze/ingest_raw_data.py

# Option 2: Use MinIO
# 1. Create bucket 'lakehouse' in MinIO console
# 2. Upload CSV files to lakehouse/raw/
# 3. Set: export USE_MINIO=true
# 4. Run: python scripts/bronze/ingest_raw_data.py
```

## Step 6: Explore Data

### Using Jupyter Notebook

1. Open http://localhost:8888
2. Navigate to `notebooks/demo.ipynb`
3. Run cells to explore Bronze layer data

### Using Spark SQL

```python
from config.spark_config import create_spark_session
spark, base_path = create_spark_session()

# Read Bronze table
bronze_df = spark.read.format("delta").load(f"{base_path}/bronze/transactions")
bronze_df.show(5)

# Query with SQL
spark.sql("SELECT * FROM bronze_transactions LIMIT 10").show()
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Restart services
docker-compose restart
```

### Port conflicts
Edit `docker-compose.yml` to change port mappings.

### Java not found
Install Java 8 or 11:
```bash
# macOS
brew install openjdk@11

# Linux
sudo apt-get install openjdk-11-jdk
```

## Next Steps

1. **Implement Silver Layer**: Clean and validate data
2. **Implement Gold Layer**: Create business aggregates
3. **Add Delta Lake Features**: Time travel, MERGE, etc.
4. **Set up Orchestration**: Create Airflow DAGs

See [PROJECT_STATUS.md](PROJECT_STATUS.md) for current progress.

## Useful Commands

```bash
# View all Makefile commands
make help

# Stop all services
make stop

# Clean up everything
make clean

# View service logs
make logs
```

## Need Help?

- Check [docs/setup.md](docs/setup.md) for detailed setup
- Check [docs/architecture.md](docs/architecture.md) for architecture details
- Check [docs/data_flow.md](docs/data_flow.md) for data flow documentation

