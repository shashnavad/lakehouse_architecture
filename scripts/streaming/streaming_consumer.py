"""
Spark Structured Streaming Consumer
Reads from Kafka and writes to Delta Lake Bronze table
Implements watermarking and exactly-once semantics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, to_timestamp,
    window, count as spark_count, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.spark_config import create_spark_session, get_table_paths


def define_transaction_schema():
    """Define schema for transaction data from Kafka"""
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


def create_streaming_query(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    bronze_path: str,
    checkpoint_location: str,
    watermark_duration: str = "10 minutes"
):
    """
    Create Spark Structured Streaming query from Kafka to Delta Lake
    
    Args:
        spark: SparkSession
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_topic: Kafka topic name
        bronze_path: Path to Bronze Delta table
        checkpoint_location: Checkpoint location for exactly-once semantics
        watermark_duration: Watermark duration for late-arriving data
    
    Returns:
        StreamingQuery object
    """
    print("=" * 60)
    print("SPARK STRUCTURED STREAMING CONSUMER")
    print("=" * 60)
    print(f"Kafka: {kafka_bootstrap_servers}")
    print(f"Topic: {kafka_topic}")
    print(f"Bronze Path: {bronze_path}")
    print(f"Checkpoint: {checkpoint_location}")
    print(f"Watermark: {watermark_duration}")
    print("=" * 60)
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON value
    schema = define_transaction_schema()
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        "kafka_key",
        "kafka_timestamp",
        "data.*"
    )
    
    # Add metadata columns
    enriched_df = parsed_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("kafka_stream")) \
        .withColumn("data_source", lit("kafka")) \
        .withColumn("ingestion_date", current_timestamp().cast("date"))
    
    # Convert transaction_date to timestamp
    enriched_df = enriched_df \
        .withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
        )
    
    # Add watermark for late-arriving data
    watermarked_df = enriched_df \
        .withWatermark("transaction_timestamp", watermark_duration)
    
    # Write to Delta Lake Bronze table
    bronze_table_path = f"{bronze_path}/transactions"
    
    query = watermarked_df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="10 seconds") \
        .start(bronze_table_path)
    
    print("\n✓ Streaming query started")
    print(f"  Query ID: {query.id}")
    print(f"  Run ID: {query.runId}")
    
    return query


def create_aggregated_streaming_query(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    output_path: str,
    checkpoint_location: str,
    window_duration: str = "1 minute",
    watermark_duration: str = "10 minutes"
):
    """
    Create aggregated streaming query with windowing
    
    Args:
        spark: SparkSession
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_topic: Kafka topic name
        output_path: Output path for aggregated results
        checkpoint_location: Checkpoint location
        window_duration: Window duration (e.g., "1 minute")
        watermark_duration: Watermark duration
    
    Returns:
        StreamingQuery object
    """
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse and enrich
    schema = define_transaction_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    enriched_df = parsed_df \
        .withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
        )
    
    # Windowed aggregation
    windowed_df = enriched_df \
        .withWatermark("transaction_timestamp", watermark_duration) \
        .groupBy(
            window(col("transaction_timestamp"), window_duration),
            col("status")
        ) \
        .agg(
            spark_count("transaction_id").alias("transaction_count"),
            spark_sum("final_amount").alias("total_revenue")
        )
    
    # Write aggregated results
    query = windowed_df \
        .writeStream \
        .format("delta") \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="30 seconds") \
        .start(output_path)
    
    return query


def main():
    """Main streaming consumer function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Consumer")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transactions", help="Kafka topic")
    parser.add_argument("--bronze-path", help="Bronze table path (overrides config)")
    parser.add_argument("--checkpoint", default="/tmp/streaming_checkpoints", help="Checkpoint location")
    parser.add_argument("--watermark", default="10 minutes", help="Watermark duration")
    parser.add_argument("--aggregated", action="store_true", help="Run aggregated query")
    parser.add_argument("--window", default="1 minute", help="Window duration (aggregated mode)")
    
    args = parser.parse_args()
    
    # Create Spark session
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    spark, base_path = create_spark_session(
        app_name="StreamingConsumer",
        use_minio=use_minio
    )
    
    # Get paths
    paths = get_table_paths(base_path)
    bronze_path = args.bronze_path or paths["bronze"]
    checkpoint_location = args.checkpoint
    
    try:
        if args.aggregated:
            print("Starting aggregated streaming query...")
            output_path = f"{paths['bronze']}/transactions_aggregated"
            query = create_aggregated_streaming_query(
                spark,
                args.kafka_servers,
                args.topic,
                output_path,
                checkpoint_location,
                args.window,
                args.watermark
            )
        else:
            print("Starting streaming consumer...")
            query = create_streaming_query(
                spark,
                args.kafka_servers,
                args.topic,
                bronze_path,
                checkpoint_location,
                args.watermark
            )
        
        # Wait for termination
        print("\n📊 Streaming query active. Press Ctrl+C to stop...")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⚠ Stopping streaming query...")
        query.stop()
        print("✓ Streaming query stopped")
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

