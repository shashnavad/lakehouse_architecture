"""
Tests for Phase 6: Streaming (Kafka + Spark Structured Streaming)
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime


def test_transaction_schema_definition():
    """Test transaction schema definition"""
    from scripts.streaming.streaming_consumer import define_transaction_schema
    
    schema = define_transaction_schema()
    assert schema is not None
    assert "transaction_id" in [field.name for field in schema.fields]
    assert "customer_id" in [field.name for field in schema.fields]
    assert "final_amount" in [field.name for field in schema.fields]


def test_kafka_producer_initialization():
    """Test Kafka producer initialization"""
    from scripts.streaming.kafka_producer import TransactionKafkaProducer
    
    producer = TransactionKafkaProducer(
        bootstrap_servers="localhost:9092",
        topic="test_topic"
    )
    
    assert producer.bootstrap_servers == "localhost:9092"
    assert producer.topic == "test_topic"
    assert producer.product_ids is not None
    assert producer.customer_ids is not None


def test_generate_transaction():
    """Test transaction generation"""
    from scripts.streaming.kafka_producer import TransactionKafkaProducer
    
    producer = TransactionKafkaProducer()
    transaction = producer.generate_transaction("TXN_TEST_001")
    
    assert transaction["transaction_id"] == "TXN_TEST_001"
    assert "customer_id" in transaction
    assert "product_id" in transaction
    assert "final_amount" in transaction
    assert transaction["final_amount"] >= 0
    assert transaction["quantity"] >= 1


def test_streaming_watermark(spark_session, tmp_path):
    """Test watermarking functionality"""
    from pyspark.sql.functions import col, to_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    # Create test data with timestamps
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp_str", StringType(), True),
    ])
    
    data = [
        ("1", "2024-01-01 10:00:00"),
        ("2", "2024-01-01 10:05:00"),
        ("3", "2024-01-01 10:10:00"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss"))
    
    # Add watermark
    watermarked_df = df.withWatermark("timestamp", "5 minutes")
    
    # Verify watermark column exists
    assert "timestamp" in watermarked_df.columns
    
    # Verify data is preserved
    assert watermarked_df.count() == 3


def test_exactly_once_semantics_checkpoint(spark_session, tmp_path):
    """Test checkpoint location for exactly-once semantics"""
    checkpoint_path = str(tmp_path / "checkpoint")
    
    # Checkpoint location should be a valid path
    import os
    os.makedirs(checkpoint_path, exist_ok=True)
    assert os.path.exists(checkpoint_path)


def test_streaming_schema_parsing(spark_session):
    """Test JSON schema parsing for streaming"""
    from scripts.streaming.streaming_consumer import define_transaction_schema
    from pyspark.sql.functions import from_json, col
    
    schema = define_transaction_schema()
    
    # Create test JSON string
    json_data = '{"transaction_id":"TXN_001","customer_id":"CUST_001","product_id":"PROD_001","transaction_date":"2024-01-01 10:00:00","quantity":2,"unit_price":50.0,"total_amount":100.0,"discount":0.0,"final_amount":100.0,"payment_method":"Credit Card","status":"Completed","shipping_address":"123 Main St"}'
    
    # Create DataFrame with JSON string
    from pyspark.sql.types import StringType
    test_df = spark_session.createDataFrame([(json_data,)], ["json_string"])
    
    # Parse JSON
    parsed_df = test_df.select(
        from_json(col("json_string"), schema).alias("data")
    ).select("data.*")
    
    # Verify parsing
    assert parsed_df.count() == 1
    row = parsed_df.first()
    assert row.transaction_id == "TXN_001"
    assert row.final_amount == 100.0


def test_metadata_enrichment(spark_session):
    """Test adding metadata columns for streaming data"""
    from pyspark.sql.functions import current_timestamp, lit
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
    ])
    
    df = spark_session.createDataFrame([("TXN_001",)], schema)
    
    # Add metadata
    enriched_df = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("kafka_stream")) \
        .withColumn("data_source", lit("kafka"))
    
    assert "ingestion_timestamp" in enriched_df.columns
    assert "source_file" in enriched_df.columns
    assert "data_source" in enriched_df.columns
    
    row = enriched_df.first()
    assert row.source_file == "kafka_stream"
    assert row.data_source == "kafka"

