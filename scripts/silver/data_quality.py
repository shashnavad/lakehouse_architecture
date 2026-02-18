"""
Data Quality Framework for Silver Layer
Implements validation rules and quality checks
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, sum as spark_sum,
    lit, current_timestamp, concat_ws
)
from pyspark.sql.types import StructType
from typing import Dict, List, Tuple
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


class DataQualityChecker:
    """Data quality validation framework"""
    
    def __init__(self):
        self.validation_errors = []
    
    def check_null_values(self, df: DataFrame, columns: List[str]) -> Tuple[DataFrame, DataFrame]:
        """
        Check for null values in specified columns
        
        Args:
            df: Input DataFrame
            columns: List of columns to check
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        # Create condition for null check
        null_condition = None
        for col_name in columns:
            if null_condition is None:
                null_condition = col(col_name).isNull()
            else:
                null_condition = null_condition | col(col_name).isNull()
        
        # Split into valid and invalid
        invalid_df = df.filter(null_condition)
        valid_df = df.filter(~null_condition)
        
        return valid_df, invalid_df
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
        """
        Check for duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: List of columns that form the unique key
        
        Returns:
            Tuple of (deduplicated_df, duplicates_df)
        """
        # Window function approach for deduplication
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy(key_columns).orderBy(col("ingestion_timestamp").desc())
        
        df_with_rank = df.withColumn("_row_num", row_number().over(window_spec))
        
        # Keep only the latest record for each key
        deduplicated_df = df_with_rank.filter(col("_row_num") == 1).drop("_row_num")
        
        # Find duplicates (records that were removed)
        duplicates_df = df_with_rank.filter(col("_row_num") > 1).drop("_row_num")
        
        return deduplicated_df, duplicates_df
    
    def check_value_ranges(
        self, 
        df: DataFrame, 
        column: str, 
        min_value: float = None, 
        max_value: float = None
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Check if values are within specified range
        
        Args:
            df: Input DataFrame
            column: Column to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        condition = None
        
        if min_value is not None:
            condition = col(column) < min_value
        
        if max_value is not None:
            if condition is None:
                condition = col(column) > max_value
            else:
                condition = condition | (col(column) > max_value)
        
        if condition is None:
            return df, df.limit(0)  # No validation, return empty invalid
        
        invalid_df = df.filter(condition)
        valid_df = df.filter(~condition)
        
        return valid_df, invalid_df
    
    def check_referential_integrity(
        self, 
        df: DataFrame, 
        foreign_key_col: str, 
        reference_df: DataFrame, 
        reference_key_col: str
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Check referential integrity (foreign key validation)
        
        Args:
            df: Input DataFrame
            foreign_key_col: Foreign key column name
            reference_df: Reference DataFrame
            reference_key_col: Reference key column name
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        # Get valid keys from reference
        valid_keys = reference_df.select(reference_key_col).distinct()
        
        # Left anti join to find invalid foreign keys
        invalid_df = df.join(
            valid_keys,
            df[foreign_key_col] == valid_keys[reference_key_col],
            "left_anti"
        )
        
        # Valid records
        valid_df = df.join(
            valid_keys,
            df[foreign_key_col] == valid_keys[reference_key_col],
            "inner"
        )
        
        return valid_df, invalid_df
    
    def add_validation_metadata(
        self, 
        df: DataFrame, 
        validation_status: str, 
        error_message: str = None
    ) -> DataFrame:
        """
        Add validation metadata to DataFrame
        
        Args:
            df: Input DataFrame
            validation_status: Status (PASSED, FAILED, QUARANTINED)
            error_message: Optional error message
        
        Returns:
            DataFrame with validation metadata
        """
        return df.withColumn("validation_status", lit(validation_status)) \
                 .withColumn("validation_timestamp", current_timestamp()) \
                 .withColumn("error_message", lit(error_message) if error_message else lit(None))


def validate_transactions(
    df: DataFrame, 
    customers_df: DataFrame = None, 
    products_df: DataFrame = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate transaction data
    
    Args:
        df: Transactions DataFrame
        customers_df: Customers reference DataFrame (optional)
        products_df: Products reference DataFrame (optional)
    
    Returns:
        Tuple of (valid_df, quarantine_df)
    """
    checker = DataQualityChecker()
    quarantine_records = []
    
    # 1. Check for nulls in required fields
    required_fields = ["transaction_id", "customer_id", "product_id", "final_amount"]
    valid_df, null_invalid = checker.check_null_values(df, required_fields)
    if null_invalid.count() > 0:
        null_invalid = checker.add_validation_metadata(
            null_invalid, 
            "FAILED", 
            "Null values in required fields"
        )
        quarantine_records.append(null_invalid)
    
    # 2. Check for duplicates
    valid_df, duplicates = checker.check_duplicates(valid_df, ["transaction_id"])
    if duplicates.count() > 0:
        duplicates = checker.add_validation_metadata(
            duplicates,
            "FAILED",
            "Duplicate transaction_id"
        )
        quarantine_records.append(duplicates)
    
    # 3. Check value ranges
    # Final amount should be positive
    valid_df, range_invalid = checker.check_value_ranges(valid_df, "final_amount", min_value=0)
    if range_invalid.count() > 0:
        range_invalid = checker.add_validation_metadata(
            range_invalid,
            "FAILED",
            "final_amount must be >= 0"
        )
        quarantine_records.append(range_invalid)
    
    # Quantity should be positive
    valid_df, qty_invalid = checker.check_value_ranges(valid_df, "quantity", min_value=1)
    if qty_invalid.count() > 0:
        qty_invalid = checker.add_validation_metadata(
            qty_invalid,
            "FAILED",
            "quantity must be >= 1"
        )
        quarantine_records.append(qty_invalid)
    
    # 4. Referential integrity checks (if reference data provided)
    if customers_df is not None:
        valid_df, fk_invalid = checker.check_referential_integrity(
            valid_df, "customer_id", customers_df, "customer_id"
        )
        if fk_invalid.count() > 0:
            fk_invalid = checker.add_validation_metadata(
                fk_invalid,
                "FAILED",
                "Invalid customer_id"
            )
            quarantine_records.append(fk_invalid)
    
    if products_df is not None:
        valid_df, fk_invalid = checker.check_referential_integrity(
            valid_df, "product_id", products_df, "product_id"
        )
        if fk_invalid.count() > 0:
            fk_invalid = checker.add_validation_metadata(
                fk_invalid,
                "FAILED",
                "Invalid product_id"
            )
            quarantine_records.append(fk_invalid)
    
    # Combine all quarantine records
    if quarantine_records:
        from functools import reduce
        quarantine_df = reduce(DataFrame.unionByName, quarantine_records)
    else:
        # Create empty DataFrame with same schema + validation columns
        quarantine_df = df.limit(0).withColumn("validation_status", lit(""))
        quarantine_df = quarantine_df.withColumn("validation_timestamp", current_timestamp())
        quarantine_df = quarantine_df.withColumn("error_message", lit(""))
    
    # Mark valid records
    valid_df = checker.add_validation_metadata(valid_df, "PASSED", None)
    
    return valid_df, quarantine_df


def validate_customers(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Validate customer data"""
    checker = DataQualityChecker()
    
    # Check required fields
    required_fields = ["customer_id", "customer_name", "email"]
    valid_df, null_invalid = checker.check_null_values(df, required_fields)
    
    # Check duplicates
    valid_df, duplicates = checker.check_duplicates(valid_df, ["customer_id"])
    
    # Combine invalid records
    quarantine_records = []
    if null_invalid.count() > 0:
        null_invalid = checker.add_validation_metadata(null_invalid, "FAILED", "Null values in required fields")
        quarantine_records.append(null_invalid)
    if duplicates.count() > 0:
        duplicates = checker.add_validation_metadata(duplicates, "FAILED", "Duplicate customer_id")
        quarantine_records.append(duplicates)
    
    if quarantine_records:
        from functools import reduce
        quarantine_df = reduce(DataFrame.unionByName, quarantine_records)
    else:
        quarantine_df = df.limit(0).withColumn("validation_status", lit(""))
        quarantine_df = quarantine_df.withColumn("validation_timestamp", current_timestamp())
        quarantine_df = quarantine_df.withColumn("error_message", lit(""))
    
    valid_df = checker.add_validation_metadata(valid_df, "PASSED", None)
    
    return valid_df, quarantine_df


def validate_products(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Validate product data"""
    checker = DataQualityChecker()
    
    # Check required fields
    required_fields = ["product_id", "product_name", "price"]
    valid_df, null_invalid = checker.check_null_values(df, required_fields)
    
    # Check duplicates
    valid_df, duplicates = checker.check_duplicates(valid_df, ["product_id"])
    
    # Check value ranges
    valid_df, price_invalid = checker.check_value_ranges(valid_df, "price", min_value=0)
    
    # Combine invalid records
    quarantine_records = []
    if null_invalid.count() > 0:
        null_invalid = checker.add_validation_metadata(null_invalid, "FAILED", "Null values in required fields")
        quarantine_records.append(null_invalid)
    if duplicates.count() > 0:
        duplicates = checker.add_validation_metadata(duplicates, "FAILED", "Duplicate product_id")
        quarantine_records.append(duplicates)
    if price_invalid.count() > 0:
        price_invalid = checker.add_validation_metadata(price_invalid, "FAILED", "price must be >= 0")
        quarantine_records.append(price_invalid)
    
    if quarantine_records:
        from functools import reduce
        quarantine_df = reduce(DataFrame.unionByName, quarantine_records)
    else:
        quarantine_df = df.limit(0).withColumn("validation_status", lit(""))
        quarantine_df = quarantine_df.withColumn("validation_timestamp", current_timestamp())
        quarantine_df = quarantine_df.withColumn("error_message", lit(""))
    
    valid_df = checker.add_validation_metadata(valid_df, "PASSED", None)
    
    return valid_df, quarantine_df

