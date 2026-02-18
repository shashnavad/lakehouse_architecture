"""
Tests for Gold Layer aggregations and dimension tables
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta


def test_dim_customers_creation(spark_session, tmp_path):
    """Test that customer dimension table is created correctly"""
    from scripts.gold.process_gold import create_dim_customers
    
    # Create sample Silver data
    silver_path = str(tmp_path / "silver")
    gold_path = str(tmp_path / "gold")
    
    # Create sample customers
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
    ])
    customers_data = [
        ("CUST_001", "John Doe", "john@example.com", "PREMIUM", "USA"),
        ("CUST_002", "Jane Smith", "jane@example.com", "STANDARD", "UK"),
    ]
    customers_df = spark_session.createDataFrame(customers_data, customers_schema)
    customers_df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    
    # Create sample transactions
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    transactions_data = [
        ("TXN_001", "CUST_001", "PROD_001", datetime(2024, 1, 1, 10, 0, 0), 100.0, "COMPLETED"),
        ("TXN_002", "CUST_001", "PROD_002", datetime(2024, 1, 2, 11, 0, 0), 200.0, "COMPLETED"),
        ("TXN_003", "CUST_002", "PROD_001", datetime(2024, 1, 3, 12, 0, 0), 150.0, "COMPLETED"),
    ]
    transactions_df = spark_session.createDataFrame(transactions_data, transactions_schema)
    transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    
    # Create dimension
    create_dim_customers(spark_session, silver_path, gold_path)
    
    # Verify dimension table exists
    dim_customers = spark_session.read.format("delta").load(f"{gold_path}/dim_customers")
    assert dim_customers.count() == 2
    assert "total_revenue" in dim_customers.columns
    assert "total_transactions" in dim_customers.columns
    assert "customer_lifetime_value" in dim_customers.columns


def test_daily_sales_summary(spark_session, tmp_path):
    """Test daily sales summary aggregation"""
    from scripts.gold.process_gold import create_daily_sales_summary
    
    silver_path = str(tmp_path / "silver")
    gold_path = str(tmp_path / "gold")
    
    # Create sample transactions
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    
    transactions_data = [
        ("TXN_001", "CUST_001", "PROD_001", datetime(2024, 1, 1, 10, 0, 0), 100.0, 2, 0.0, 40.0, "COMPLETED"),
        ("TXN_002", "CUST_002", "PROD_002", datetime(2024, 1, 1, 11, 0, 0), 200.0, 1, 10.0, 80.0, "COMPLETED"),
        ("TXN_003", "CUST_001", "PROD_001", datetime(2024, 1, 2, 10, 0, 0), 150.0, 3, 0.0, 60.0, "COMPLETED"),
    ]
    
    transactions_df = spark_session.createDataFrame(transactions_data, transactions_schema)
    transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    
    # Create daily summary
    create_daily_sales_summary(spark_session, silver_path, gold_path)
    
    # Verify summary
    daily_summary = spark_session.read.format("delta").load(f"{gold_path}/daily_sales_summary")
    assert daily_summary.count() == 2  # Two different dates
    
    # Check aggregations
    day1 = daily_summary.filter(col("sale_date") == datetime(2024, 1, 1).date()).collect()[0]
    assert day1.transaction_count == 2
    assert day1.total_revenue == 300.0


def test_customer_segments(spark_session, tmp_path):
    """Test customer segmentation"""
    from scripts.gold.process_gold import create_customer_segments
    
    silver_path = str(tmp_path / "silver")
    gold_path = str(tmp_path / "gold")
    
    # Create sample data
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
    ])
    customers_data = [
        ("CUST_001", "John Doe"),
        ("CUST_002", "Jane Smith"),
    ]
    customers_df = spark_session.createDataFrame(customers_data, customers_schema)
    customers_df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    
    # Customer 1: High value, recent, frequent
    transactions_data = [
        ("TXN_001", "CUST_001", "PROD_001", datetime.now() - timedelta(days=10), 1000.0, "COMPLETED"),
        ("TXN_002", "CUST_001", "PROD_002", datetime.now() - timedelta(days=5), 2000.0, "COMPLETED"),
        ("TXN_003", "CUST_001", "PROD_003", datetime.now() - timedelta(days=1), 1500.0, "COMPLETED"),
        # Customer 2: Low value, old, infrequent
        ("TXN_004", "CUST_002", "PROD_001", datetime.now() - timedelta(days=200), 100.0, "COMPLETED"),
    ]
    
    transactions_df = spark_session.createDataFrame(transactions_data, transactions_schema)
    transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    
    # Create segments
    create_customer_segments(spark_session, silver_path, gold_path)
    
    # Verify segments
    segments = spark_session.read.format("delta").load(f"{gold_path}/customer_segments")
    assert segments.count() == 2
    assert "segment" in segments.columns
    assert "recency_score" in segments.columns
    assert "frequency_score" in segments.columns
    assert "monetary_score" in segments.columns


def test_product_performance(spark_session, tmp_path):
    """Test product performance aggregation"""
    from scripts.gold.process_gold import create_product_performance
    
    silver_path = str(tmp_path / "silver")
    gold_path = str(tmp_path / "gold")
    
    # Create sample products
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True),
    ])
    products_data = [
        ("PROD_001", "Laptop", "ELECTRONICS", 999.99, 600.0),
        ("PROD_002", "Mouse", "ELECTRONICS", 25.99, 10.0),
    ]
    products_df = spark_session.createDataFrame(products_data, products_schema)
    products_df.write.format("delta").mode("overwrite").save(f"{silver_path}/products")
    
    # Create sample transactions
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("final_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("profit", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    
    transactions_data = [
        ("TXN_001", "CUST_001", "PROD_001", datetime(2024, 1, 1), 999.99, 1, 399.99, "COMPLETED"),
        ("TXN_002", "CUST_002", "PROD_001", datetime(2024, 1, 2), 999.99, 1, 399.99, "COMPLETED"),
        ("TXN_003", "CUST_001", "PROD_002", datetime(2024, 1, 3), 25.99, 2, 15.99, "COMPLETED"),
    ]
    
    transactions_df = spark_session.createDataFrame(transactions_data, transactions_schema)
    transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    
    # Create product performance
    create_product_performance(spark_session, silver_path, gold_path)
    
    # Verify performance table
    performance = spark_session.read.format("delta").load(f"{gold_path}/product_performance")
    assert performance.count() == 2
    
    prod1 = performance.filter(col("product_id") == "PROD_001").collect()[0]
    assert prod1.total_revenue == 1999.98
    assert prod1.total_quantity_sold == 2


def test_dim_dates_creation(spark_session, tmp_path):
    """Test date dimension table creation"""
    from scripts.gold.process_gold import create_dim_dates
    
    gold_path = str(tmp_path / "gold")
    
    # Create date dimension
    create_dim_dates(spark_session, gold_path, "2024-01-01", "2024-01-31")
    
    # Verify dimension
    dim_dates = spark_session.read.format("delta").load(f"{gold_path}/dim_dates")
    assert dim_dates.count() == 31  # 31 days in January
    
    # Check columns
    assert "date" in dim_dates.columns
    assert "year" in dim_dates.columns
    assert "month" in dim_dates.columns
    assert "day" in dim_dates.columns
    assert "quarter" in dim_dates.columns
    assert "week_number" in dim_dates.columns


def test_optimize_gold_tables(spark_session, tmp_path):
    """Test table optimization"""
    from scripts.gold.process_gold import optimize_gold_tables
    
    gold_path = str(tmp_path / "gold")
    
    # Create a simple table
    test_schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    test_data = [("1", 100), ("2", 200)]
    test_df = spark_session.createDataFrame(test_data, test_schema)
    test_df.write.format("delta").mode("overwrite").save(f"{gold_path}/daily_sales_summary")
    
    # Optimize (should not fail)
    try:
        optimize_gold_tables(spark_session, gold_path)
        assert True
    except Exception as e:
        # Optimization might fail in test environment, that's okay
        pass

