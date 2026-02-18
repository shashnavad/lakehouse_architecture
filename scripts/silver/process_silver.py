"""
Silver Layer: Data Cleaning and Enrichment Pipeline
Reads from Bronze, applies quality checks, transforms data, and writes to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace,
    to_date, current_timestamp, lit, coalesce,
    round as spark_round
)
from pyspark.sql.types import TimestampType
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.spark_config import create_spark_session, get_table_paths
from scripts.silver.data_quality import (
    validate_transactions, validate_customers, validate_products
)


def transform_transactions(df):
    """
    Transform transaction data:
    - Standardize column names
    - Add derived columns
    - Clean and standardize values
    """
    # Standardize status values
    df = df.withColumn(
        "status",
        upper(trim(col("status")))
    )
    
    # Add derived columns
    df = df.withColumn(
        "profit",
        col("final_amount") - (col("unit_price") * col("quantity") * 0.6)  # Assuming 60% cost
    )
    
    df = df.withColumn(
        "discount_percentage",
        when(col("total_amount") > 0, 
             (col("discount") / col("total_amount")) * 100)
        .otherwise(lit(0))
    )
    
    # Round monetary values
    df = df.withColumn("final_amount", spark_round(col("final_amount"), 2))
    df = df.withColumn("total_amount", spark_round(col("total_amount"), 2))
    df = df.withColumn("discount", spark_round(col("discount"), 2))
    df = df.withColumn("profit", spark_round(col("profit"), 2))
    
    # Convert transaction_date to proper timestamp
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Add processing metadata
    df = df.withColumn("silver_processed_timestamp", current_timestamp())
    df = df.withColumn("silver_processed_date", current_timestamp().cast("date"))
    
    return df


def transform_customers(df):
    """Transform customer data"""
    # Standardize text fields
    df = df.withColumn("customer_name", trim(upper(col("customer_name"))))
    df = df.withColumn("email", lower(trim(col("email"))))
    df = df.withColumn("segment", upper(trim(col("segment"))))
    df = df.withColumn("country", upper(trim(col("country"))))
    
    # Add processing metadata
    df = df.withColumn("silver_processed_timestamp", current_timestamp())
    df = df.withColumn("silver_processed_date", current_timestamp().cast("date"))
    
    return df


def transform_products(df):
    """Transform product data"""
    # Standardize text fields
    df = df.withColumn("product_name", trim(col("product_name")))
    df = df.withColumn("category", upper(trim(col("category"))))
    
    # Calculate profit margin
    df = df.withColumn(
        "profit_margin",
        when(col("price") > 0,
             ((col("price") - col("cost")) / col("price")) * 100)
        .otherwise(lit(0))
    )
    
    # Round monetary values
    df = df.withColumn("price", spark_round(col("price"), 2))
    df = df.withColumn("cost", spark_round(col("cost"), 2))
    df = df.withColumn("profit_margin", spark_round(col("profit_margin"), 2))
    
    # Add processing metadata
    df = df.withColumn("silver_processed_timestamp", current_timestamp())
    df = df.withColumn("silver_processed_date", current_timestamp().cast("date"))
    
    return df


def get_last_processed_timestamp(spark: SparkSession, table_path: str) -> datetime:
    """
    Get the last processed timestamp from Silver table for incremental processing
    
    Args:
        spark: SparkSession
        table_path: Path to Silver Delta table
    
    Returns:
        Last processed timestamp or None if table doesn't exist
    """
    try:
        # Check if table exists
        silver_df = spark.read.format("delta").load(table_path)
        
        if silver_df.count() > 0:
            # Get max processed timestamp
            from pyspark.sql.functions import max as spark_max
            result = silver_df.agg(spark_max("silver_processed_timestamp")).collect()
            if result and result[0][0]:
                return result[0][0]
    except Exception:
        # Table doesn't exist yet, return None
        pass
    
    return None


def process_transactions_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
    incremental: bool = True
):
    """Process transactions from Bronze to Silver"""
    print("=" * 60)
    print("PROCESSING TRANSACTIONS: BRONZE → SILVER")
    print("=" * 60)
    
    # Read from Bronze
    bronze_table_path = f"{bronze_path}/transactions"
    print(f"Reading from Bronze: {bronze_table_path}")
    
    bronze_df = spark.read.format("delta").load(bronze_table_path)
    
    # Incremental processing
    if incremental:
        last_processed = get_last_processed_timestamp(spark, f"{silver_path}/transactions")
        if last_processed:
            print(f"Incremental mode: Processing records after {last_processed}")
            bronze_df = bronze_df.filter(col("ingestion_timestamp") > lit(last_processed))
        else:
            print("Full processing mode: No previous Silver data found")
    
    initial_count = bronze_df.count()
    print(f"Records to process: {initial_count}")
    
    if initial_count == 0:
        print("No new records to process")
        return
    
    # Read reference data for validation
    try:
        customers_df = spark.read.format("delta").load(f"{bronze_path}/customers")
        products_df = spark.read.format("delta").load(f"{bronze_path}/products")
    except Exception:
        print("Warning: Reference data not available, skipping referential integrity checks")
        customers_df = None
        products_df = None
    
    # Validate data
    print("\nApplying data quality checks...")
    valid_df, quarantine_df = validate_transactions(
        bronze_df, 
        customers_df, 
        products_df
    )
    
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    
    print(f"✓ Valid records: {valid_count}")
    print(f"✗ Quarantined records: {quarantine_count}")
    
    # Transform valid data
    print("\nTransforming data...")
    silver_df = transform_transactions(valid_df)
    
    # Write to Silver (MERGE operation for upserts)
    silver_table_path = f"{silver_path}/transactions"
    print(f"\nWriting to Silver: {silver_table_path}")
    
    # Use MERGE for upserts based on transaction_id
    from delta.tables import DeltaTable
    
    try:
        # Try to read existing table
        delta_table = DeltaTable.forPath(spark, silver_table_path)
        
        # MERGE operation
        delta_table.alias("target").merge(
            silver_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print("✓ MERGE operation completed")
    except Exception:
        # Table doesn't exist, create it
        print("Creating new Silver table...")
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_table_path)
        print("✓ Silver table created")
    
    # Write quarantine records
    if quarantine_count > 0:
        quarantine_table_path = f"{quarantine_path}/transactions"
        print(f"\nWriting quarantine records: {quarantine_table_path}")
        
        quarantine_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(quarantine_table_path)
        
        print(f"✓ {quarantine_count} records quarantined")
    
    print("\n" + "=" * 60)
    print("✓ TRANSACTIONS PROCESSING COMPLETE")
    print("=" * 60)


def process_customers_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
    incremental: bool = True
):
    """Process customers from Bronze to Silver"""
    print("=" * 60)
    print("PROCESSING CUSTOMERS: BRONZE → SILVER")
    print("=" * 60)
    
    bronze_table_path = f"{bronze_path}/customers"
    bronze_df = spark.read.format("delta").load(bronze_table_path)
    
    if incremental:
        last_processed = get_last_processed_timestamp(spark, f"{silver_path}/customers")
        if last_processed:
            bronze_df = bronze_df.filter(col("ingestion_timestamp") > lit(last_processed))
    
    initial_count = bronze_df.count()
    print(f"Records to process: {initial_count}")
    
    if initial_count == 0:
        print("No new records to process")
        return
    
    # Validate
    valid_df, quarantine_df = validate_customers(bronze_df)
    
    # Transform
    silver_df = transform_customers(valid_df)
    
    # Write to Silver
    silver_table_path = f"{silver_path}/customers"
    
    from delta.tables import DeltaTable
    try:
        delta_table = DeltaTable.forPath(spark, silver_table_path)
        delta_table.alias("target").merge(
            silver_df.alias("source"),
            "target.customer_id = source.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        silver_df.write.format("delta").mode("overwrite").save(silver_table_path)
    
    # Write quarantine
    if quarantine_df.count() > 0:
        quarantine_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{quarantine_path}/customers")
    
    print("✓ CUSTOMERS PROCESSING COMPLETE")


def process_products_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
    incremental: bool = True
):
    """Process products from Bronze to Silver"""
    print("=" * 60)
    print("PROCESSING PRODUCTS: BRONZE → SILVER")
    print("=" * 60)
    
    bronze_table_path = f"{bronze_path}/products"
    bronze_df = spark.read.format("delta").load(bronze_table_path)
    
    if incremental:
        last_processed = get_last_processed_timestamp(spark, f"{silver_path}/products")
        if last_processed:
            bronze_df = bronze_df.filter(col("ingestion_timestamp") > lit(last_processed))
    
    initial_count = bronze_df.count()
    print(f"Records to process: {initial_count}")
    
    if initial_count == 0:
        print("No new records to process")
        return
    
    # Validate
    valid_df, quarantine_df = validate_products(bronze_df)
    
    # Transform
    silver_df = transform_products(valid_df)
    
    # Write to Silver
    silver_table_path = f"{silver_path}/products"
    
    from delta.tables import DeltaTable
    try:
        delta_table = DeltaTable.forPath(spark, silver_table_path)
        delta_table.alias("target").merge(
            silver_df.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        silver_df.write.format("delta").mode("overwrite").save(silver_table_path)
    
    # Write quarantine
    if quarantine_df.count() > 0:
        quarantine_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{quarantine_path}/products")
    
    print("✓ PRODUCTS PROCESSING COMPLETE")


def main(incremental: bool = True):
    """Main Silver layer processing function"""
    # Create Spark session
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    spark, base_path = create_spark_session(
        app_name="SilverProcessing",
        use_minio=use_minio
    )
    
    # Get table paths
    paths = get_table_paths(base_path)
    
    try:
        # Process all tables
        process_transactions_silver(
            spark,
            paths["bronze"],
            paths["silver"],
            paths["quarantine"],
            incremental=incremental
        )
        
        process_customers_silver(
            spark,
            paths["bronze"],
            paths["silver"],
            paths["quarantine"],
            incremental=incremental
        )
        
        process_products_silver(
            spark,
            paths["bronze"],
            paths["silver"],
            paths["quarantine"],
            incremental=incremental
        )
        
        print("\n" + "=" * 60)
        print("✓ SILVER LAYER PROCESSING COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error during Silver processing: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process Silver layer")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full processing (not incremental)"
    )
    args = parser.parse_args()
    
    main(incremental=not args.full)

