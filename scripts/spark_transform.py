import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, year, month, dayofmonth, weekofyear, quarter, dayofweek, datediff, when, expr

def create_spark_session():
    """Initializes and returns a Spark Session configured for Spark Cluster."""
    spark = SparkSession.builder \
        .appName("OlistDataWarehouseETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ==========================================
# 1. Define Schemas for Ingestion
# ==========================================

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_length", StringType(), True),
    StructField("product_description_length", StringType(), True),
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True)
])

sellers_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True)
])

order_payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True)
])

order_reviews_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True)
])

translation_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True)
])

def read_kafka_topic(spark, topic_name, schema, bootstrap_servers="kafka:29092"):
    """Reads records from Kafka and parses them into Spark DataFrame with schema."""
    print(f"Reading from Kafka topic: {topic_name}...")
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json("json_str", schema).alias("data")) \
        .select("data.*")
    
    return parsed_df

def main():
    spark = create_spark_session()
    
    # Define directories
    output_dir = "/workspace/tmp_transformed"
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Ingest Data from Kafka
    df_customers_raw = read_kafka_topic(spark, "olist_customers", customers_schema)
    df_products_raw = read_kafka_topic(spark, "olist_products", products_schema)
    df_sellers_raw = read_kafka_topic(spark, "olist_sellers", sellers_schema)
    df_orders_raw = read_kafka_topic(spark, "olist_orders", orders_schema)
    df_order_items_raw = read_kafka_topic(spark, "olist_order_items", order_items_schema)
    df_payments_raw = read_kafka_topic(spark, "olist_order_payments", order_payments_schema)
    df_reviews_raw = read_kafka_topic(spark, "olist_order_reviews", order_reviews_schema)
    df_translation = read_kafka_topic(spark, "product_category_translation", translation_schema)

    print("Kafka Ingestion completed. Starting transformations...")

    # ==========================================
    # 3. Model Dimension Tables
    # ==========================================

    # Dimension: Customers
    print("Modeling dim_customers...")
    dim_customers = df_customers_raw \
        .withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast(IntegerType())) \
        .dropDuplicates(["customer_id"]) \
        .filter(col("customer_id").isNotNull())

    # Dimension: Products (Joined with english translations)
    print("Modeling dim_products...")
    dim_products = df_products_raw \
        .join(df_translation, on="product_category_name", how="left") \
        .withColumn("product_name_length", col("product_name_length").cast(IntegerType())) \
        .withColumn("product_description_length", col("product_description_length").cast(IntegerType())) \
        .withColumn("product_photos_qty", col("product_photos_qty").cast(IntegerType())) \
        .withColumn("product_weight_g", col("product_weight_g").cast(IntegerType())) \
        .withColumn("product_length_cm", col("product_length_cm").cast(IntegerType())) \
        .withColumn("product_height_cm", col("product_height_cm").cast(IntegerType())) \
        .withColumn("product_width_cm", col("product_width_cm").cast(IntegerType())) \
        .dropDuplicates(["product_id"]) \
        .filter(col("product_id").isNotNull())

    # Dimension: Sellers
    print("Modeling dim_sellers...")
    dim_sellers = df_sellers_raw \
        .withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast(IntegerType())) \
        .dropDuplicates(["seller_id"]) \
        .filter(col("seller_id").isNotNull())

    # Dimension: Reviews
    print("Modeling dim_reviews...")
    dim_reviews = df_reviews_raw \
        .withColumn("review_score", col("review_score").cast(IntegerType())) \
        .withColumn("review_creation_date", to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .dropDuplicates(["review_id"]) \
        .filter(col("review_id").isNotNull()) \
        .select(
            "review_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        )

    # Dimension: Time (Generate calendar parts from orders)
    print("Modeling dim_time...")
    # Extract unique timestamps from all order status dates
    order_timestamps = df_orders_raw.select(to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss").alias("time_key")).distinct()
    
    dim_time = order_timestamps \
        .filter(col("time_key").isNotNull()) \
        .withColumn("date", to_date("time_key")) \
        .withColumn("day", dayofmonth("time_key")) \
        .withColumn("week", weekofyear("time_key")) \
        .withColumn("month", month("time_key")) \
        .withColumn("quarter", quarter("time_key")) \
        .withColumn("year", year("time_key")) \
        .withColumn("day_of_week", dayofweek("time_key")) \
        .withColumn("is_weekend", when(dayofweek("time_key").isin(1, 7), True).otherwise(False))

    # ==========================================
    # 4. Model Fact Table (Sales Transactions)
    # ==========================================
    print("Modeling fact_sales_transactions...")

    # Join Order Items with Orders, Payments, and Reviews
    df_items_cast = df_order_items_raw \
        .withColumn("order_item_id", col("order_item_id").cast(IntegerType())) \
        .withColumn("price", col("price").cast(DecimalType(10, 2))) \
        .withColumn("freight_value", col("freight_value").cast(DecimalType(10, 2)))

    df_orders_cast = df_orders_raw \
        .withColumn("purchase_timestamp", to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("approved_timestamp", to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("delivered_carrier_date", to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("delivered_customer_date", to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("estimated_delivery_date", to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss"))

    df_payments_cast = df_payments_raw \
        .withColumn("payment_sequential", col("payment_sequential").cast(IntegerType())) \
        .withColumn("payment_installments", col("payment_installments").cast(IntegerType())) \
        .withColumn("payment_value", col("payment_value").cast(DecimalType(10, 2))) \
        .dropDuplicates(["order_id"]) # Deduplicate to prevent order multiplication in star schema

    df_reviews_dedup = df_reviews_raw.dropDuplicates(["order_id"])

    # 4.1 Base Joins
    fact_sales = df_items_cast \
        .join(df_orders_cast, on="order_id", how="inner") \
        .join(df_payments_cast, on="order_id", how="left") \
        .join(df_reviews_dedup, on="order_id", how="left")

    # 4.2 Feature Engineering & Data Quality Checks
    fact_sales = fact_sales \
        .withColumn(
            "shipping_duration_days",
            datediff(col("delivered_customer_date"), col("purchase_timestamp"))
        ) \
        .withColumn(
            "delivery_delay_days",
            when(
                col("delivered_customer_date") > col("estimated_delivery_date"),
                datediff(col("delivered_customer_date"), col("estimated_delivery_date"))
            ).otherwise(0)
        ) \
        .filter(
            col("order_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            col("seller_id").isNotNull() &
            (col("price") >= 0)  # Data Quality bounds check
        ) \
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "review_id",
            "purchase_timestamp",
            "approved_timestamp",
            "delivered_carrier_date",
            "delivered_customer_date",
            "estimated_delivery_date",
            "price",
            "freight_value",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            col("order_status").alias("order_status"),
            "shipping_duration_days",
            "delivery_delay_days"
        )

    # ==========================================
    # 5. Save Transformed Data (Parquet Verification)
    # ==========================================
    print("Writing dimension and fact tables to temporary Parquet storage...")
    
    dim_customers.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_customers"))
    dim_products.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_products"))
    dim_sellers.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_sellers"))
    dim_reviews.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_reviews"))
    dim_time.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_time"))
    fact_sales.write.mode("overwrite").parquet(os.path.join(output_dir, "fact_sales_transactions"))

    print("All transformations and Parquet writes completed successfully!")
    spark.stop()

if __name__ == '__main__':
    main()
