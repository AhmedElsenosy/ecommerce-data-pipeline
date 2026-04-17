"""
E-Commerce Data Pipeline - Spark ETL Job
=========================================
This script reads raw CSV files from HDFS, cleans and transforms
the data, then saves processed results back to HDFS as Parquet files.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, round as spark_round,
    count, sum as spark_sum, avg, desc, when, lit,
    to_timestamp, upper, trim, lower
)


# ============================================
# 1. CREATE SPARK SESSION
# ============================================
# This is the entry point to Spark. It connects to the Spark master
# and tells it where HDFS is running.

spark = SparkSession.builder \
    .appName("ECommerce-ETL-Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Set log level to show only warnings and errors (less noise)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("STARTING E-COMMERCE ETL PIPELINE")
print("=" * 60)


# ============================================
# 2. EXTRACT - Read raw CSV files from HDFS
# ============================================
# header=True  → first row is column names
# inferSchema=True → Spark auto-detects data types (int, string, etc.)

print("\n[EXTRACT] Reading raw CSV files from HDFS...")

users_df = spark.read.csv("hdfs://namenode:9000/data/raw/users.csv", header=True, inferSchema=True)
products_df = spark.read.csv("hdfs://namenode:9000/data/raw/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("hdfs://namenode:9000/data/raw/orders.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("hdfs://namenode:9000/data/raw/order_items.csv", header=True, inferSchema=True)
reviews_df = spark.read.csv("hdfs://namenode:9000/data/raw/reviews.csv", header=True, inferSchema=True)
events_df = spark.read.csv("hdfs://namenode:9000/data/raw/events.csv", header=True, inferSchema=True)

print(f"  users:       {users_df.count()} rows")
print(f"  products:    {products_df.count()} rows")
print(f"  orders:      {orders_df.count()} rows")
print(f"  order_items: {order_items_df.count()} rows")
print(f"  reviews:     {reviews_df.count()} rows")
print(f"  events:      {events_df.count()} rows")


# ============================================
# 3. TRANSFORM - Clean the data
# ============================================
print("\n[TRANSFORM] Cleaning data...")

# --- Clean Users ---
# Remove duplicates by user_id, trim whitespace from names, standardize gender
users_clean = users_df \
    .dropDuplicates(["user_id"]) \
    .dropna(subset=["user_id", "email"]) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("gender", upper(trim(col("gender")))) \
    .withColumn("city", trim(col("city")))

print(f"  Users after cleaning: {users_clean.count()} rows")

# --- Clean Products ---
# Remove duplicates, drop rows without product_id or price
products_clean = products_df \
    .dropDuplicates(["product_id"]) \
    .dropna(subset=["product_id", "price"]) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", trim(col("category"))) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("rating", col("rating").cast("double"))

print(f"  Products after cleaning: {products_clean.count()} rows")

# --- Clean Orders ---
# Parse order_date to timestamp, remove duplicates
orders_clean = orders_df \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id", "user_id"]) \
    .withColumn("order_date", to_timestamp(col("order_date"))) \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("order_status", upper(trim(col("order_status"))))

print(f"  Orders after cleaning: {orders_clean.count()} rows")

# --- Clean Order Items ---
order_items_clean = order_items_df \
    .dropDuplicates(["order_item_id"]) \
    .dropna(subset=["order_id", "product_id"]) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("item_price", col("item_price").cast("double"))

print(f"  Order Items after cleaning: {order_items_clean.count()} rows")

# --- Clean Reviews ---
reviews_clean = reviews_df \
    .dropDuplicates(["review_id"]) \
    .dropna(subset=["review_id", "product_id"]) \
    .withColumn("rating", col("rating").cast("double")) \
    .withColumn("review_date", to_timestamp(col("review_date")))

print(f"  Reviews after cleaning: {reviews_clean.count()} rows")

# --- Clean Events ---
events_clean = events_df \
    .dropDuplicates(["event_id"]) \
    .dropna(subset=["event_id", "user_id"]) \
    .withColumn("event_type", lower(trim(col("event_type")))) \
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

print(f"  Events after cleaning: {events_clean.count()} rows")


# ============================================
# 4. TRANSFORM - Join tables
# ============================================
print("\n[TRANSFORM] Joining tables...")

# --- Full Order Details ---
# Join: order_items + orders + products + users
# This gives us: who bought what, when, for how much
# Join step by step to avoid duplicate column names
order_details = order_items_clean \
    .join(orders_clean, ["order_id"], "inner") \
    .join(products_clean, ["product_id"], "inner")

# Drop duplicate user_id if it exists before joining with users
existing_cols = order_details.columns
if existing_cols.count("user_id") > 1:
    order_details = order_details.toDF(*[f"{c}_{i}" if existing_cols[:i].count(c) > 0 else c for i, c in enumerate(existing_cols)])

order_details = order_details \
    .join(users_clean, ["user_id"], "inner") \
    .withColumn("total_item_price", spark_round(col("quantity") * col("item_price"), 2)) \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_day", dayofmonth(col("order_date")))


print(f"  Order Details (joined): {order_details.count()} rows")

# --- Reviews with Product Info ---
# Join: reviews + products (so we know which product each review is for)
reviews_with_products = reviews_clean \
    .withColumnRenamed("rating", "review_rating") \
    .join(
        products_clean.withColumnRenamed("rating", "product_rating"),
        ["product_id"],
        "inner"
    )

print(f"  Reviews with Products: {reviews_with_products.count()} rows")


# ============================================
# 5. TRANSFORM - Build aggregation tables
# ============================================
print("\n[TRANSFORM] Building aggregation tables...")

# --- Monthly Sales ---
# Total revenue and number of orders per month
monthly_sales = order_details \
    .groupBy("order_year", "order_month") \
    .agg(
        spark_round(spark_sum("total_item_price"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        spark_round(avg("total_item_price"), 2).alias("avg_order_value")
    ) \
    .orderBy("order_year", "order_month")

print(f"  Monthly Sales: {monthly_sales.count()} rows")

# --- Sales by Category ---
# Revenue and units sold per product category
category_sales = order_details \
    .groupBy("category") \
    .agg(
        spark_round(spark_sum("total_item_price"), 2).alias("total_revenue"),
        spark_sum("quantity").alias("total_units_sold"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy(desc("total_revenue"))

print(f"  Category Sales: {category_sales.count()} rows")

# --- Top Products ---
# Best selling products by revenue
top_products = order_details \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        spark_round(spark_sum("total_item_price"), 2).alias("total_revenue"),
        spark_sum("quantity").alias("total_units_sold"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy(desc("total_revenue"))

print(f"  Top Products: {top_products.count()} rows")

# --- Customer Summary ---
# Total spending, order count per customer
customer_summary = order_details \
    .groupBy("user_id", "name", "email", "gender", "city") \
    .agg(
        spark_round(spark_sum("total_item_price"), 2).alias("total_spent"),
        count("order_id").alias("total_orders"),
        spark_round(avg("total_item_price"), 2).alias("avg_order_value")
    ) \
    .orderBy(desc("total_spent"))

print(f"  Customer Summary: {customer_summary.count()} rows")

# --- Order Status Breakdown ---
# How many orders are completed, cancelled, returned
order_status = orders_clean \
    .groupBy("order_status") \
    .agg(
        count("order_id").alias("order_count"),
        spark_round(spark_sum("total_amount"), 2).alias("total_amount")
    ) \
    .orderBy(desc("order_count"))

print(f"  Order Status: {order_status.count()} rows")

# --- Event Funnel ---
# Count of each event type (view, cart, wishlist, purchase)
event_funnel = events_clean \
    .groupBy("event_type") \
    .agg(count("event_id").alias("event_count")) \
    .orderBy(desc("event_count"))

print(f"  Event Funnel: {event_funnel.count()} rows")

# --- City Sales ---
# Revenue by city
city_sales = order_details \
    .groupBy("city") \
    .agg(
        spark_round(spark_sum("total_item_price"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        spark_round(avg("total_item_price"), 2).alias("avg_order_value")
    ) \
    .orderBy(desc("total_revenue"))

print(f"  City Sales: {city_sales.count()} rows")


# ============================================
# 6. LOAD - Save to HDFS as Parquet
# ============================================
print("\n[LOAD] Saving processed data to HDFS...")

HDFS_OUTPUT = "hdfs://namenode:9000/data/processed"

# Save cleaned datasets
order_details.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/order_details")
reviews_with_products.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/reviews_with_products")
events_clean.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/events_clean")

# Save aggregation tables
monthly_sales.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/monthly_sales")
category_sales.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/category_sales")
top_products.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/top_products")
customer_summary.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/customer_summary")
order_status.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/order_status")
event_funnel.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/event_funnel")
city_sales.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/city_sales")

print("\n" + "=" * 60)
print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 60)
print(f"\nOutput saved to: {HDFS_OUTPUT}/")
print("Datasets created:")
print("  - order_details (joined order+items+products+users)")
print("  - reviews_with_products")
print("  - events_clean")
print("  - monthly_sales")
print("  - category_sales")
print("  - top_products")
print("  - customer_summary")
print("  - order_status")
print("  - event_funnel")
print("  - city_sales")

# Stop Spark session
spark.stop()
