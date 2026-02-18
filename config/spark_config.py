"""
Spark Configuration for Lakehouse Architecture
Supports both MinIO (S3-compatible) and local filesystem
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os


def create_spark_session(
    app_name: str = "LakehousePipeline",
    use_minio: bool = True,
    minio_endpoint: str = "http://localhost:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin"
) -> SparkSession:
    """
    Create and configure Spark session with Delta Lake support
    
    Args:
        app_name: Name of the Spark application
        use_minio: Whether to use MinIO (True) or local filesystem (False)
        minio_endpoint: MinIO endpoint URL
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
    
    Returns:
        Configured SparkSession with Delta Lake support
    """
    conf = SparkConf()
    
    # Core Spark configuration
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Delta Lake configuration
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Configure for MinIO (S3-compatible)
    if use_minio:
        conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        conf.set("spark.hadoop.fs.s3a.access.key", minio_access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # Use S3A for Delta Lake
        base_path = "s3a://lakehouse"
    else:
        # Use local filesystem
        base_path = "file:///tmp/lakehouse"
        os.makedirs("/tmp/lakehouse", exist_ok=True)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark, base_path


def get_table_paths(base_path: str) -> dict:
    """
    Get standardized table paths for Bronze, Silver, and Gold layers
    
    Args:
        base_path: Base path for storage (S3A or local)
    
    Returns:
        Dictionary with table paths
    """
    return {
        "bronze": f"{base_path}/bronze",
        "silver": f"{base_path}/silver",
        "gold": f"{base_path}/gold",
        "quarantine": f"{base_path}/quarantine"
    }

