"""
Bronze Layer: Raw Data Ingestion Pipeline
Reads raw data files and writes to Delta Lake Bronze table with metadata
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, input_file_name, 
    regexp_extract, when, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.spark_config import create_spark_session, get_table_paths


def define_transaction_schema():
    """Define schema for transaction data"""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("shipping_address", StringType(), True)
    ])


def ingest_transactions(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    table_name: str = "transactions"
):
    """
    Ingest transaction data into Bronze layer
    
    Args:
        spark: SparkSession
        source_path: Path to source CSV file
        bronze_path: Path to Bronze Delta table
        table_name: Name of the Delta table
    """
    print(f"Reading raw data from: {source_path}")
    
    # Read CSV with schema
    schema = define_transaction_schema()
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(source_path)
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("data_source", lit("ecommerce")) \
        .withColumn("ingestion_date", current_timestamp().cast("date"))
    
    # Convert transaction_date to timestamp
    df_with_metadata = df_with_metadata \
        .withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
        )
    
    # Write to Delta Lake Bronze table
    bronze_table_path = f"{bronze_path}/{table_name}"
    
    print(f"Writing to Bronze table: {bronze_table_path}")
    
    df_with_metadata.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_table_path)
    
    # Register as table for querying
    df_with_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"bronze_{table_name}")
    
    print(f"✓ Successfully ingested {df_with_metadata.count()} records to Bronze layer")
    
    return df_with_metadata


def ingest_customers(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    table_name: str = "customers"
):
    """Ingest customer data into Bronze layer"""
    print(f"Reading customer data from: {source_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)
    
    # Add metadata
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("data_source", lit("ecommerce")) \
        .withColumn("ingestion_date", current_timestamp().cast("date"))
    
    bronze_table_path = f"{bronze_path}/{table_name}"
    
    df_with_metadata.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_table_path)
    
    print(f"✓ Successfully ingested {df_with_metadata.count()} customers to Bronze layer")
    
    return df_with_metadata


def ingest_products(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    table_name: str = "products"
):
    """Ingest product data into Bronze layer"""
    print(f"Reading product data from: {source_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)
    
    # Add metadata
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("data_source", lit("ecommerce")) \
        .withColumn("ingestion_date", current_timestamp().cast("date"))
    
    bronze_table_path = f"{bronze_path}/{table_name}"
    
    df_with_metadata.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_table_path)
    
    print(f"✓ Successfully ingested {df_with_metadata.count()} products to Bronze layer")
    
    return df_with_metadata


def main():
    """Main ingestion function"""
    # Create Spark session
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    spark, base_path = create_spark_session(
        app_name="BronzeIngestion",
        use_minio=use_minio
    )
    
    # Get table paths
    paths = get_table_paths(base_path)
    
    # Source data paths (adjust based on your setup)
    if use_minio:
        source_base = "s3a://lakehouse/raw"
    else:
        source_base = "file:///tmp/lakehouse/raw"
        # Ensure directory exists
        import os
        os.makedirs("/tmp/lakehouse/raw", exist_ok=True)
    
    # Ingest all tables
    try:
        print("=" * 60)
        print("BRONZE LAYER INGESTION")
        print("=" * 60)
        
        # Ingest transactions
        ingest_transactions(
            spark,
            f"{source_base}/transactions.csv",
            paths["bronze"],
            "transactions"
        )
        
        # Ingest customers
        ingest_customers(
            spark,
            f"{source_base}/customers.csv",
            paths["bronze"],
            "customers"
        )
        
        # Ingest products
        ingest_products(
            spark,
            f"{source_base}/products.csv",
            paths["bronze"],
            "products"
        )
        
        print("\n" + "=" * 60)
        print("✓ BRONZE INGESTION COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error during ingestion: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

