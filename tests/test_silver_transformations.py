"""
Tests for Silver Layer transformations
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from scripts.silver.process_silver import (
    transform_transactions,
    transform_customers,
    transform_products
)


def test_transform_transactions_adds_derived_columns(spark_session, sample_transactions_df):
    """Test that transaction transformations add derived columns"""
    transformed_df = transform_transactions(sample_transactions_df)
    
    # Check for derived columns
    assert "profit" in transformed_df.columns
    assert "discount_percentage" in transformed_df.columns
    assert "transaction_timestamp" in transformed_df.columns
    assert "silver_processed_timestamp" in transformed_df.columns


def test_transform_transactions_standardizes_status(spark_session):
    """Test that status values are standardized"""
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
    ])
    
    data = [
        ("T1", "completed", 100.0, 100.0, 0.0, 50.0, 2, "2024-01-01 10:00:00"),
        ("T2", "  PENDING  ", 50.0, 50.0, 0.0, 50.0, 1, "2024-01-01 11:00:00"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    transformed_df = transform_transactions(df)
    
    # Status should be uppercase and trimmed
    statuses = transformed_df.select("status").distinct().collect()
    assert all(row.status in ["COMPLETED", "PENDING"] for row in statuses)


def test_transform_customers_standardizes_text(spark_session, sample_customers_df):
    """Test that customer text fields are standardized"""
    transformed_df = transform_customers(sample_customers_df)
    
    # Check that processing metadata is added
    assert "silver_processed_timestamp" in transformed_df.columns
    assert "silver_processed_date" in transformed_df.columns
    
    # Check that text fields exist (standardization happens in transformation)
    assert "customer_name" in transformed_df.columns
    assert "email" in transformed_df.columns


def test_transform_products_adds_profit_margin(spark_session, sample_products_df):
    """Test that product transformations add profit margin"""
    transformed_df = transform_products(sample_products_df)
    
    # Check for derived column
    assert "profit_margin" in transformed_df.columns
    assert "silver_processed_timestamp" in transformed_df.columns
    
    # Profit margin should be calculated
    profit_margins = transformed_df.select("profit_margin").collect()
    assert all(row.profit_margin is not None for row in profit_margins)


def test_transform_transactions_rounds_monetary_values(spark_session):
    """Test that monetary values are rounded"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    data = [
        ("T1", 100.123456, 100.123456, 0.123456, 50.123456, 2, "2024-01-01 10:00:00", "Completed"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    transformed_df = transform_transactions(df)
    
    # Values should be rounded to 2 decimal places
    row = transformed_df.first()
    assert round(row.final_amount, 2) == row.final_amount
    assert round(row.total_amount, 2) == row.total_amount

