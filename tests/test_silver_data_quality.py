"""
Tests for Silver Layer data quality checks
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from scripts.silver.data_quality import (
    DataQualityChecker,
    validate_transactions,
    validate_customers,
    validate_products
)


def test_null_value_check(spark_session):
    """Test null value detection"""
    checker = DataQualityChecker()
    
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    
    data = [
        ("1", "John", 25),
        ("2", None, 30),  # Null name
        ("3", "Jane", None),  # Null age
        ("4", "Bob", 35),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    valid_df, invalid_df = checker.check_null_values(df, ["name", "age"])
    
    assert valid_df.count() == 2  # Only rows 1 and 4 are valid
    assert invalid_df.count() == 2  # Rows 2 and 3 have nulls


def test_duplicate_detection(spark_session):
    """Test duplicate detection"""
    checker = DataQualityChecker()
    
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ])
    
    data = [
        ("1", "John"),
        ("2", "Jane"),
        ("1", "John Duplicate"),  # Duplicate ID
        ("3", "Bob"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    deduplicated_df, duplicates_df = checker.check_duplicates(df, ["id"])
    
    # Should keep 3 unique records (latest for each ID)
    assert deduplicated_df.count() == 3
    assert duplicates_df.count() == 1  # One duplicate removed


def test_value_range_check(spark_session):
    """Test value range validation"""
    checker = DataQualityChecker()
    
    from pyspark.sql.types import StructType, StructField, DoubleType
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    
    data = [
        ("1", 10.0),
        ("2", -5.0),  # Invalid: negative
        ("3", 100.0),
        ("4", 200.0),  # Invalid: exceeds max
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    valid_df, invalid_df = checker.check_value_ranges(df, "price", min_value=0, max_value=150)
    
    assert valid_df.count() == 2  # IDs 1 and 3
    assert invalid_df.count() == 2  # IDs 2 and 4


def test_referential_integrity(spark_session):
    """Test foreign key validation"""
    checker = DataQualityChecker()
    
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Reference table
    ref_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
    ])
    ref_data = [("C1", "John"), ("C2", "Jane")]
    ref_df = spark_session.createDataFrame(ref_data, ref_schema)
    
    # Transaction table
    trans_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
    ])
    trans_data = [
        ("T1", "C1"),  # Valid
        ("T2", "C2"),  # Valid
        ("T3", "C3"),  # Invalid: doesn't exist in reference
    ]
    trans_df = spark_session.createDataFrame(trans_data, trans_schema)
    
    valid_df, invalid_df = checker.check_referential_integrity(
        trans_df, "customer_id", ref_df, "customer_id"
    )
    
    assert valid_df.count() == 2  # T1 and T2
    assert invalid_df.count() == 1  # T3


def test_validate_transactions(spark_session, sample_transactions_df, sample_customers_df, sample_products_df):
    """Test transaction validation"""
    valid_df, quarantine_df = validate_transactions(
        sample_transactions_df,
        sample_customers_df,
        sample_products_df
    )
    
    # All sample transactions should be valid
    assert valid_df.count() > 0
    assert "validation_status" in valid_df.columns
    assert "error_message" in valid_df.columns


def test_validate_transactions_with_invalid_data(spark_session):
    """Test transaction validation with invalid data"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
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
        StructField("shipping_address", StringType(), True),
    ])
    
    # Invalid data: null transaction_id, negative amount, zero quantity
    data = [
        (None, "C1", "P1", "2024-01-01", 2, 50.0, 100.0, 0.0, 100.0, "CC", "Completed", "123 St"),  # Null ID
        ("T2", "C1", "P1", "2024-01-01", 0, 50.0, 100.0, 0.0, 100.0, "CC", "Completed", "123 St"),  # Zero quantity
        ("T3", "C1", "P1", "2024-01-01", 2, 50.0, 100.0, 0.0, -50.0, "CC", "Completed", "123 St"),  # Negative amount
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_file", lit("test.csv"))
    df = df.withColumn("data_source", lit("test"))
    
    valid_df, quarantine_df = validate_transactions(df, None, None)
    
    # All should be quarantined
    assert quarantine_df.count() == 3
    assert valid_df.count() == 0


def test_validate_customers(spark_session, sample_customers_df):
    """Test customer validation"""
    valid_df, quarantine_df = validate_customers(sample_customers_df)
    
    assert valid_df.count() > 0
    assert "validation_status" in valid_df.columns


def test_validate_products(spark_session, sample_products_df):
    """Test product validation"""
    valid_df, quarantine_df = validate_products(sample_products_df)
    
    assert valid_df.count() > 0
    assert "validation_status" in valid_df.columns

