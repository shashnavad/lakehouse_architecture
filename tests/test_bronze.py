"""
Tests for Bronze Layer ingestion
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os
import tempfile
import shutil


def test_bronze_schema_enforcement(spark_session, sample_transactions_df):
    """Test that Bronze layer enforces schema"""
    # Schema should be defined and enforced
    assert sample_transactions_df.schema is not None
    assert "transaction_id" in sample_transactions_df.columns
    assert "customer_id" in sample_transactions_df.columns
    assert "ingestion_timestamp" in sample_transactions_df.columns


def test_bronze_metadata_columns(sample_transactions_df):
    """Test that metadata columns are added"""
    required_metadata = ["ingestion_timestamp", "source_file", "data_source"]
    
    for col_name in required_metadata:
        assert col_name in sample_transactions_df.columns, f"Missing metadata column: {col_name}"


def test_bronze_write_read_delta(spark_session, sample_transactions_df, tmp_path):
    """Test writing to and reading from Delta Lake"""
    delta_path = str(tmp_path / "bronze_transactions")
    
    # Write to Delta
    sample_transactions_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_path)
    
    # Read from Delta
    read_df = spark_session.read.format("delta").load(delta_path)
    
    # Verify data
    assert read_df.count() == sample_transactions_df.count()
    assert "transaction_id" in read_df.columns
    assert "ingestion_timestamp" in read_df.columns


def test_bronze_append_mode(spark_session, sample_transactions_df, tmp_path):
    """Test append mode for Bronze layer"""
    delta_path = str(tmp_path / "bronze_append")
    
    # Initial write
    sample_transactions_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_path)
    
    initial_count = spark_session.read.format("delta").load(delta_path).count()
    
    # Append more data
    sample_transactions_df.write \
        .format("delta") \
        .mode("append") \
        .save(delta_path)
    
    final_count = spark_session.read.format("delta").load(delta_path).count()
    
    # Should have doubled
    assert final_count == initial_count * 2


def test_bronze_data_types(sample_transactions_df):
    """Test that data types are correct"""
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    
    # Check key column types
    assert sample_transactions_df.schema["transaction_id"].dataType == StringType()
    assert sample_transactions_df.schema["quantity"].dataType == IntegerType()
    assert sample_transactions_df.schema["final_amount"].dataType == DoubleType()

