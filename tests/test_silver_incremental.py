"""
Tests for Silver Layer incremental processing
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, timedelta
import tempfile
import shutil


def test_incremental_processing_filters_by_timestamp(spark_session, sample_transactions_df, tmp_path):
    """Test that incremental processing only processes new records"""
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    # Create initial Bronze data
    bronze_df = sample_transactions_df
    bronze_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/transactions")
    
    # Process first time (full load)
    from scripts.silver.process_silver import process_transactions_silver
    process_transactions_silver(
        spark_session,
        bronze_path,
        silver_path,
        str(tmp_path / "quarantine"),
        incremental=True
    )
    
    initial_silver_count = spark_session.read.format("delta").load(f"{silver_path}/transactions").count()
    
    # Add new records with later timestamp
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    new_data = [
        ("TXN_NEW_001", "CUST_00001", "PROD_00001", "2024-01-02 10:00:00", 1, 50.0, 50.0, 0.0, 50.0, "Credit Card", "Completed", "123 St"),
    ]
    
    new_df = spark_session.createDataFrame(new_data, sample_transactions_df.schema)
    new_df = new_df.withColumn("ingestion_timestamp", current_timestamp())
    new_df = new_df.withColumn("source_file", lit("new.csv"))
    new_df = new_df.withColumn("data_source", lit("test"))
    
    # Append to Bronze
    new_df.write.format("delta").mode("append").save(f"{bronze_path}/transactions")
    
    # Process again (incremental)
    process_transactions_silver(
        spark_session,
        bronze_path,
        silver_path,
        str(tmp_path / "quarantine"),
        incremental=True
    )
    
    final_silver_count = spark_session.read.format("delta").load(f"{silver_path}/transactions").count()
    
    # Should have increased by 1
    assert final_silver_count == initial_silver_count + 1


def test_full_processing_ignores_timestamps(spark_session, sample_transactions_df, tmp_path):
    """Test that full processing mode processes all records"""
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    # Create Bronze data
    bronze_df = sample_transactions_df
    bronze_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/transactions")
    
    # Process with full mode
    from scripts.silver.process_silver import process_transactions_silver
    process_transactions_silver(
        spark_session,
        bronze_path,
        silver_path,
        str(tmp_path / "quarantine"),
        incremental=False
    )
    
    silver_count = spark_session.read.format("delta").load(f"{silver_path}/transactions").count()
    
    # Should process all records
    assert silver_count == bronze_df.count()

