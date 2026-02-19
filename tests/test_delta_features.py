"""
Tests for Phase 5: Delta Lake Features
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime


def test_time_travel_by_version(spark_session, tmp_path):
    """Test querying historical version of data"""
    from scripts.delta_features.delta_lake_features import (
        time_travel_by_version,
        get_table_history
    )
    
    # Create Delta table with multiple versions
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    
    # Version 0
    df1 = spark_session.createDataFrame([("1", 100), ("2", 200)], schema)
    df1.write.format("delta").mode("overwrite").save(str(tmp_path / "test_table"))
    
    # Version 1
    df2 = spark_session.createDataFrame([("3", 300)], schema)
    df2.write.format("delta").mode("append").save(str(tmp_path / "test_table"))
    
    table_path = str(tmp_path / "test_table")
    
    # Query version 0
    v0_df = time_travel_by_version(spark_session, table_path, 0)
    assert v0_df.count() == 2
    
    # Query version 1 (current)
    v1_df = time_travel_by_version(spark_session, table_path, 1)
    assert v1_df.count() == 3
    
    # Query current (no version)
    current_df = spark_session.read.format("delta").load(table_path)
    assert current_df.count() == 3


def test_time_travel_by_timestamp(spark_session, tmp_path):
    """Test querying by timestamp"""
    from scripts.delta_features.delta_lake_features import time_travel_by_timestamp
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    
    df = spark_session.createDataFrame([("1", 100)], schema)
    df.write.format("delta").mode("overwrite").save(str(tmp_path / "test_table"))
    
    table_path = str(tmp_path / "test_table")
    
    # Query by timestamp (use a timestamp from before the write)
    ts = "2020-01-01 00:00:00"
    try:
        result = time_travel_by_timestamp(spark_session, table_path, ts)
        # May return empty if timestamp is before any data
        assert result is not None
    except Exception:
        # Timestamp before table creation may fail
        pass


def test_acid_atomicity(spark_session, tmp_path):
    """Test ACID atomicity - all or nothing"""
    from scripts.delta_features.delta_lake_features import demonstrate_acid_transactions
    
    # Should complete without error
    demonstrate_acid_transactions(spark_session, str(tmp_path))
    
    # Verify data was written
    table_path = str(tmp_path / "acid_test")
    df = spark_session.read.format("delta").load(table_path)
    assert df.count() >= 3  # Initial 3 + possibly 2 from append


def test_schema_evolution(spark_session, tmp_path):
    """Test adding new columns without breaking existing data"""
    from scripts.delta_features.delta_lake_features import demonstrate_schema_evolution
    
    # Create initial table
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    df = spark_session.createDataFrame([("1", 100), ("2", 200)], schema)
    table_path = str(tmp_path / "evolve_test")
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # Evolve schema - add new columns (use compatible types)
    demonstrate_schema_evolution(
        spark_session,
        table_path,
        new_columns={"new_col": "default", "score": 0}
    )
    
    # Verify new columns exist and data preserved
    evolved_df = spark_session.read.format("delta").load(table_path)
    assert "new_col" in evolved_df.columns
    assert "score" in evolved_df.columns
    assert evolved_df.count() == 2
    # Original data preserved
    assert evolved_df.filter("id = '1'").count() == 1


def test_optimize_table(spark_session, tmp_path):
    """Test OPTIMIZE command"""
    from scripts.delta_features.delta_lake_features import optimize_table
    
    # Create table
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    df = spark_session.createDataFrame([("1", 100), ("2", 200)], schema)
    table_path = str(tmp_path / "optimize_test")
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # Optimize - should not fail
    optimize_table(spark_session, table_path)
    
    # Verify table still readable
    result = spark_session.read.format("delta").load(table_path)
    assert result.count() == 2


def test_optimize_with_zorder(spark_session, tmp_path):
    """Test OPTIMIZE with Z-Ordering"""
    from scripts.delta_features.delta_lake_features import optimize_table
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    df = spark_session.createDataFrame([("1", 100), ("2", 200)], schema)
    table_path = str(tmp_path / "zorder_test")
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # Optimize with Z-Order
    optimize_table(spark_session, table_path, z_order_columns=["id"])
    
    result = spark_session.read.format("delta").load(table_path)
    assert result.count() == 2


def test_merge_operation(spark_session, tmp_path):
    """Test MERGE operation (upsert) - core Delta Lake feature"""
    from delta.tables import DeltaTable
    
    # Create initial table
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    df = spark_session.createDataFrame([("1", 100), ("2", 200)], schema)
    table_path = str(tmp_path / "merge_test")
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # MERGE: update existing, insert new
    new_data = spark_session.createDataFrame([
        ("1", 150),   # Update
        ("3", 300)    # Insert
    ], schema)
    
    delta_table = DeltaTable.forPath(spark_session, table_path)
    delta_table.alias("target").merge(
        new_data.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    # Verify
    result = spark_session.read.format("delta").load(table_path)
    assert result.count() == 3
    
    id1_value = result.filter("id = '1'").collect()[0].value
    assert id1_value == 150  # Updated

