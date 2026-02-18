"""
Pytest configuration and fixtures for testing
"""

import pytest
import os
import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing
    Uses local mode with minimal resources
    """
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.master", "local[2]")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    
    spark = SparkSession.builder \
        .appName("LakehouseTests") \
        .config(conf=conf) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory):
    """Create temporary directory for test data"""
    return tmp_path_factory.mktemp("test_data")


@pytest.fixture
def sample_transactions_df(spark_session):
    """Create sample transactions DataFrame"""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        DoubleType, TimestampType
    )
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
    
    data = [
        ("TXN_00000001", "CUST_00001", "PROD_00001", "2024-01-01 10:00:00", 2, 50.0, 100.0, 0.0, 100.0, "Credit Card", "Completed", "123 Main St"),
        ("TXN_00000002", "CUST_00002", "PROD_00002", "2024-01-01 11:00:00", 1, 75.0, 75.0, 10.0, 65.0, "PayPal", "Completed", "456 Oak Ave"),
        ("TXN_00000003", "CUST_00001", "PROD_00003", "2024-01-01 12:00:00", 3, 30.0, 90.0, 0.0, 90.0, "Debit Card", "Pending", "123 Main St"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_file", spark_session.createDataFrame([("test.csv",)], ["file"]).select("file").first()[0])
    df = df.withColumn("data_source", spark_session.createDataFrame([("test",)], ["source"]).select("source").first()[0])
    
    return df


@pytest.fixture
def sample_customers_df(spark_session):
    """Create sample customers DataFrame"""
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
    ])
    
    data = [
        ("CUST_00001", "John Doe", "john@example.com", "Premium", "USA"),
        ("CUST_00002", "Jane Smith", "jane@example.com", "Standard", "UK"),
        ("CUST_00003", "Bob Johnson", "bob@example.com", "Budget", "Canada"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    return df


@pytest.fixture
def sample_products_df(spark_session):
    """Create sample products DataFrame"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    from pyspark.sql.functions import current_timestamp
    
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
    ])
    
    data = [
        ("PROD_00001", "Laptop", "Electronics", 999.99, 600.0, 50),
        ("PROD_00002", "Mouse", "Electronics", 25.99, 10.0, 200),
        ("PROD_00003", "Keyboard", "Electronics", 79.99, 30.0, 150),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    return df

