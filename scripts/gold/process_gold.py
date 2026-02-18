"""
Gold Layer: Business Aggregates and Dimension Tables
Reads from Silver, creates business-level aggregations and dimension tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    countDistinct, date_format, to_date, year, month, dayofmonth,
    weekofyear, quarter, when, lit, round as spark_round,
    collect_list, first, last, desc, asc, row_number, coalesce
)
from pyspark.sql.window import Window
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.spark_config import create_spark_session, get_table_paths


def create_dim_customers(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create customer dimension table (SCD Type 1 - overwrite)
    Includes customer attributes and calculated metrics
    """
    print("=" * 60)
    print("CREATING DIMENSION: DIM_CUSTOMERS")
    print("=" * 60)
    
    # Read from Silver
    silver_customers = spark.read.format("delta").load(f"{silver_path}/customers")
    
    # Read transaction data for customer metrics
    try:
        silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
        
        # Calculate customer metrics
        customer_metrics = silver_transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy("customer_id") \
            .agg(
                spark_sum("final_amount").alias("total_revenue"),
                count("transaction_id").alias("total_transactions"),
                avg("final_amount").alias("avg_transaction_value"),
                spark_max("transaction_timestamp").alias("last_transaction_date"),
                spark_min("transaction_timestamp").alias("first_transaction_date")
            )
        
        # Join with customer data
        dim_customers = silver_customers \
            .join(customer_metrics, "customer_id", "left") \
            .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0.0))) \
            .withColumn("total_transactions", coalesce(col("total_transactions"), lit(0))) \
            .withColumn("avg_transaction_value", coalesce(col("avg_transaction_value"), lit(0.0))) \
            .withColumn("customer_lifetime_value", col("total_revenue")) \
            .withColumn("gold_processed_timestamp", lit(datetime.now()))
        
    except Exception as e:
        print(f"Warning: Could not calculate customer metrics: {e}")
        dim_customers = silver_customers \
            .withColumn("total_revenue", lit(0.0)) \
            .withColumn("total_transactions", lit(0)) \
            .withColumn("avg_transaction_value", lit(0.0)) \
            .withColumn("customer_lifetime_value", lit(0.0)) \
            .withColumn("gold_processed_timestamp", lit(datetime.now()))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/dim_customers"
    print(f"Writing to: {gold_table_path}")
    
    from delta.tables import DeltaTable
    try:
        delta_table = DeltaTable.forPath(spark, gold_table_path)
        delta_table.alias("target").merge(
            dim_customers.alias("source"),
            "target.customer_id = source.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✓ MERGE operation completed")
    except Exception:
        dim_customers.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_table_path)
        print("✓ Dimension table created")
    
    print(f"✓ Processed {dim_customers.count()} customers")
    print("=" * 60)


def create_dim_products(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create product dimension table
    Includes product attributes and performance metrics
    """
    print("=" * 60)
    print("CREATING DIMENSION: DIM_PRODUCTS")
    print("=" * 60)
    
    # Read from Silver
    silver_products = spark.read.format("delta").load(f"{silver_path}/products")
    
    # Read transaction data for product metrics
    try:
        silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
        
        # Calculate product metrics
        product_metrics = silver_transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy("product_id") \
            .agg(
                spark_sum("final_amount").alias("total_revenue"),
                spark_sum("quantity").alias("total_quantity_sold"),
                count("transaction_id").alias("total_transactions"),
                avg("final_amount").alias("avg_sale_price"),
                spark_max("transaction_timestamp").alias("last_sale_date")
            )
        
        # Join with product data
        dim_products = silver_products \
            .join(product_metrics, "product_id", "left") \
            .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0.0))) \
            .withColumn("total_quantity_sold", coalesce(col("total_quantity_sold"), lit(0))) \
            .withColumn("total_transactions", coalesce(col("total_transactions"), lit(0))) \
            .withColumn("avg_sale_price", coalesce(col("avg_sale_price"), lit(0.0))) \
            .withColumn("gold_processed_timestamp", lit(datetime.now()))
        
    except Exception as e:
        print(f"Warning: Could not calculate product metrics: {e}")
        dim_products = silver_products \
            .withColumn("total_revenue", lit(0.0)) \
            .withColumn("total_quantity_sold", lit(0)) \
            .withColumn("total_transactions", lit(0)) \
            .withColumn("avg_sale_price", lit(0.0)) \
            .withColumn("gold_processed_timestamp", lit(datetime.now()))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/dim_products"
    print(f"Writing to: {gold_table_path}")
    
    from delta.tables import DeltaTable
    try:
        delta_table = DeltaTable.forPath(spark, gold_table_path)
        delta_table.alias("target").merge(
            dim_products.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✓ MERGE operation completed")
    except Exception:
        dim_products.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_table_path)
        print("✓ Dimension table created")
    
    print(f"✓ Processed {dim_products.count()} products")
    print("=" * 60)


def create_dim_dates(spark: SparkSession, gold_path: str, start_date: str = "2024-01-01", end_date: str = "2024-12-31"):
    """
    Create date dimension table
    Includes date attributes for time-based analysis
    """
    print("=" * 60)
    print("CREATING DIMENSION: DIM_DATES")
    print("=" * 60)
    
    from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
    
    # Generate date range
    dates = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    
    while current <= end:
        dates.append((
            current.date(),
            current.year,
            current.month,
            current.day,
            current.strftime("%B"),  # Month name
            current.strftime("%A"),  # Day name
            current.isocalendar()[1],  # Week number
            (current.month - 1) // 3 + 1,  # Quarter
            current.strftime("%Y-%m"),  # Year-Month
            1 if current.weekday() < 5 else 0,  # Is weekday
        ))
        current = datetime(current.year, current.month, current.day) + timedelta(days=1)
    
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("day_name", StringType(), True),
        StructField("week_number", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("year_month", StringType(), True),
        StructField("is_weekday", IntegerType(), True),
    ])
    
    dim_dates = spark.createDataFrame(dates, schema)
    dim_dates = dim_dates.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/dim_dates"
    print(f"Writing to: {gold_table_path}")
    
    dim_dates.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_table_path)
    
    print(f"✓ Created {dim_dates.count()} date records")
    print("=" * 60)


def create_daily_sales_summary(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create daily sales summary fact table
    Aggregates sales by day with key metrics
    """
    print("=" * 60)
    print("CREATING AGGREGATION: DAILY_SALES_SUMMARY")
    print("=" * 60)
    
    # Read from Silver
    silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    
    # Filter completed transactions
    completed_transactions = silver_transactions.filter(col("status") == "COMPLETED")
    
    # Extract date from timestamp
    transactions_with_date = completed_transactions \
        .withColumn("sale_date", to_date(col("transaction_timestamp")))
    
    # Daily aggregations
    daily_summary = transactions_with_date \
        .groupBy("sale_date") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            spark_sum("final_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity_sold"),
            avg("final_amount").alias("avg_transaction_value"),
            spark_sum("discount").alias("total_discount"),
            spark_sum("profit").alias("total_profit"),
            spark_max("final_amount").alias("max_transaction_value"),
            spark_min("final_amount").alias("min_transaction_value")
        ) \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("month", month(col("sale_date"))) \
        .withColumn("quarter", quarter(col("sale_date"))) \
        .withColumn("week_number", weekofyear(col("sale_date"))) \
        .withColumn("gold_processed_timestamp", lit(datetime.now())) \
        .orderBy(desc("sale_date"))
    
    # Write to Gold with partitioning by year and month
    gold_table_path = f"{gold_path}/daily_sales_summary"
    print(f"Writing to: {gold_table_path}")
    
    daily_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(gold_table_path)
    
    print(f"✓ Created daily summary for {daily_summary.count()} days")
    print("=" * 60)


def create_weekly_sales_summary(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create weekly sales summary
    Aggregates sales by week
    """
    print("=" * 60)
    print("CREATING AGGREGATION: WEEKLY_SALES_SUMMARY")
    print("=" * 60)
    
    # Read from Silver
    silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    
    completed_transactions = silver_transactions.filter(col("status") == "COMPLETED")
    
    transactions_with_date = completed_transactions \
        .withColumn("sale_date", to_date(col("transaction_timestamp"))) \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("week_number", weekofyear(col("sale_date")))
    
    # Weekly aggregations
    weekly_summary = transactions_with_date \
        .groupBy("year", "week_number") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            spark_sum("final_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity_sold"),
            avg("final_amount").alias("avg_transaction_value"),
            spark_sum("profit").alias("total_profit")
        ) \
        .withColumn("gold_processed_timestamp", lit(datetime.now())) \
        .orderBy(desc("year"), desc("week_number"))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/weekly_sales_summary"
    print(f"Writing to: {gold_table_path}")
    
    weekly_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year") \
        .save(gold_table_path)
    
    print(f"✓ Created weekly summary for {weekly_summary.count()} weeks")
    print("=" * 60)


def create_monthly_sales_summary(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create monthly sales summary
    Aggregates sales by month
    """
    print("=" * 60)
    print("CREATING AGGREGATION: MONTHLY_SALES_SUMMARY")
    print("=" * 60)
    
    # Read from Silver
    silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    
    completed_transactions = silver_transactions.filter(col("status") == "COMPLETED")
    
    transactions_with_date = completed_transactions \
        .withColumn("sale_date", to_date(col("transaction_timestamp"))) \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("month", month(col("sale_date"))) \
        .withColumn("quarter", quarter(col("sale_date")))
    
    # Monthly aggregations
    monthly_summary = transactions_with_date \
        .groupBy("year", "month", "quarter") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            spark_sum("final_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity_sold"),
            avg("final_amount").alias("avg_transaction_value"),
            spark_sum("profit").alias("total_profit")
        ) \
        .withColumn("gold_processed_timestamp", lit(datetime.now())) \
        .orderBy(desc("year"), desc("month"))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/monthly_sales_summary"
    print(f"Writing to: {gold_table_path}")
    
    monthly_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "quarter") \
        .save(gold_table_path)
    
    print(f"✓ Created monthly summary for {monthly_summary.count()} months")
    print("=" * 60)


def create_customer_segments(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create customer segmentation table
    Segments customers based on RFM analysis (Recency, Frequency, Monetary)
    """
    print("=" * 60)
    print("CREATING AGGREGATION: CUSTOMER_SEGMENTS")
    print("=" * 60)
    
    # Read from Silver
    silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    silver_customers = spark.read.format("delta").load(f"{silver_path}/customers")
    
    completed_transactions = silver_transactions.filter(col("status") == "COMPLETED")
    
    # Calculate RFM metrics
    from pyspark.sql.functions import datediff, current_date
    
    rfm_metrics = completed_transactions \
        .groupBy("customer_id") \
        .agg(
            spark_sum("final_amount").alias("monetary_value"),
            count("transaction_id").alias("frequency"),
            spark_max("transaction_timestamp").alias("last_transaction_date")
        ) \
        .withColumn("recency_days", datediff(current_date(), to_date(col("last_transaction_date"))))
    
    # Create RFM scores (1-5 scale)
    rfm_scored = rfm_metrics \
        .withColumn(
            "recency_score",
            when(col("recency_days") <= 30, 5)
            .when(col("recency_days") <= 60, 4)
            .when(col("recency_days") <= 90, 3)
            .when(col("recency_days") <= 180, 2)
            .otherwise(1)
        ) \
        .withColumn(
            "frequency_score",
            when(col("frequency") >= 20, 5)
            .when(col("frequency") >= 10, 4)
            .when(col("frequency") >= 5, 3)
            .when(col("frequency") >= 2, 2)
            .otherwise(1)
        ) \
        .withColumn(
            "monetary_score",
            when(col("monetary_value") >= 5000, 5)
            .when(col("monetary_value") >= 2000, 4)
            .when(col("monetary_value") >= 1000, 3)
            .when(col("monetary_value") >= 500, 2)
            .otherwise(1)
        )
    
    # Create customer segments
    customer_segments = rfm_scored \
        .withColumn(
            "segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "At Risk")
            .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "Lost Customers")
            .when((col("recency_score") >= 3) & (col("frequency_score") <= 2), "Potential Loyalists")
            .otherwise("Need Attention")
        ) \
        .join(silver_customers.select("customer_id", "customer_name", "segment as original_segment"), "customer_id", "left") \
        .withColumn("gold_processed_timestamp", lit(datetime.now()))
    
    # Write to Gold
    gold_table_path = f"{gold_path}/customer_segments"
    print(f"Writing to: {gold_table_path}")
    
    customer_segments.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_table_path)
    
    print(f"✓ Created segments for {customer_segments.count()} customers")
    print("=" * 60)


def create_product_performance(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Create product performance table
    Aggregates product sales and performance metrics
    """
    print("=" * 60)
    print("CREATING AGGREGATION: PRODUCT_PERFORMANCE")
    print("=" * 60)
    
    # Read from Silver
    silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    silver_products = spark.read.format("delta").load(f"{silver_path}/products")
    
    completed_transactions = silver_transactions.filter(col("status") == "COMPLETED")
    
    # Product performance metrics
    product_performance = completed_transactions \
        .groupBy("product_id") \
        .agg(
            spark_sum("final_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_sum("profit").alias("total_profit"),
            count("transaction_id").alias("total_transactions"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("final_amount").alias("avg_sale_price"),
            spark_max("transaction_timestamp").alias("last_sale_date"),
            spark_min("transaction_timestamp").alias("first_sale_date")
        ) \
        .join(silver_products.select("product_id", "product_name", "category", "price", "cost"), "product_id", "left") \
        .withColumn("profit_margin", (col("total_profit") / col("total_revenue")) * 100) \
        .withColumn("gold_processed_timestamp", lit(datetime.now())) \
        .orderBy(desc("total_revenue"))
    
    # Write to Gold with partitioning by category
    gold_table_path = f"{gold_path}/product_performance"
    print(f"Writing to: {gold_table_path}")
    
    product_performance.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("category") \
        .save(gold_table_path)
    
    print(f"✓ Created performance metrics for {product_performance.count()} products")
    print("=" * 60)


def optimize_gold_tables(spark: SparkSession, gold_path: str):
    """
    Optimize Gold layer tables using Delta Lake OPTIMIZE
    Compacts small files and improves query performance
    """
    print("=" * 60)
    print("OPTIMIZING GOLD TABLES")
    print("=" * 60)
    
    tables = [
        "daily_sales_summary",
        "weekly_sales_summary",
        "monthly_sales_summary",
        "customer_segments",
        "product_performance",
        "dim_customers",
        "dim_products",
        "dim_dates"
    ]
    
    for table in tables:
        table_path = f"{gold_path}/{table}"
        try:
            print(f"Optimizing {table}...")
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            print(f"✓ Optimized {table}")
        except Exception as e:
            print(f"⚠ Could not optimize {table}: {e}")
    
    print("=" * 60)


def main():
    """Main Gold layer processing function"""
    # Create Spark session
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    spark, base_path = create_spark_session(
        app_name="GoldProcessing",
        use_minio=use_minio
    )
    
    # Get table paths
    paths = get_table_paths(base_path)
    
    try:
        print("\n" + "=" * 60)
        print("GOLD LAYER PROCESSING")
        print("=" * 60)
        
        # Create dimension tables
        create_dim_customers(spark, paths["silver"], paths["gold"])
        create_dim_products(spark, paths["silver"], paths["gold"])
        
        # Get date range from data
        try:
            silver_transactions = spark.read.format("delta").load(f"{paths['silver']}/transactions")
            date_range = silver_transactions \
                .select(spark_min("transaction_timestamp").alias("min_date"),
                       spark_max("transaction_timestamp").alias("max_date")) \
                .collect()[0]
            
            start_date = date_range.min_date.strftime("%Y-%m-%d") if date_range.min_date else "2024-01-01"
            end_date = date_range.max_date.strftime("%Y-%m-%d") if date_range.max_date else "2024-12-31"
        except:
            start_date = "2024-01-01"
            end_date = "2024-12-31"
        
        create_dim_dates(spark, paths["gold"], start_date, end_date)
        
        # Create fact tables and aggregations
        create_daily_sales_summary(spark, paths["silver"], paths["gold"])
        create_weekly_sales_summary(spark, paths["silver"], paths["gold"])
        create_monthly_sales_summary(spark, paths["silver"], paths["gold"])
        create_customer_segments(spark, paths["silver"], paths["gold"])
        create_product_performance(spark, paths["silver"], paths["gold"])
        
        # Optimize tables
        optimize_gold_tables(spark, paths["gold"])
        
        print("\n" + "=" * 60)
        print("✓ GOLD LAYER PROCESSING COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error during Gold processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

