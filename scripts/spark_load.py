import os
import sys
from pyspark.sql import SparkSession

def create_spark_session():
    """Initializes and returns a Spark Session."""
    spark = SparkSession.builder \
        .appName("OlistDataWarehouseLoader") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_table_to_postgres(df, table_name, url, properties):
    """
    Helper function to load a Spark DataFrame into a PostgreSQL table via JDBC.
    """
    row_count = df.count()
    print(f"Preparing to load {row_count} records into PostgreSQL table: {table_name}...")
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .mode("append") \
            .save()
        print(f"Successfully loaded {row_count} records into {table_name}!")
        return True
    except Exception as e:
        print(f"Failed to load data into {table_name}: {e}")
        return False

def main():
    spark = create_spark_session()
    
    # 1. Database Connection Parameters
    db_url = "jdbc:postgresql://postgres:5432/olist_dwh"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    parquet_base_dir = "/workspace/tmp_transformed"
    
    # 2. Reading Cleaned Parquet Datasets
    print("Reading transformed Parquet datasets from disk...")
    df_customers = spark.read.parquet(os.path.join(parquet_base_dir, "dim_customers"))
    df_products = spark.read.parquet(os.path.join(parquet_base_dir, "dim_products"))
    df_sellers = spark.read.parquet(os.path.join(parquet_base_dir, "dim_sellers"))
    df_reviews = spark.read.parquet(os.path.join(parquet_base_dir, "dim_reviews"))
    df_time = spark.read.parquet(os.path.join(parquet_base_dir, "dim_time"))
    df_fact = spark.read.parquet(os.path.join(parquet_base_dir, "fact_sales_transactions"))
    
    # ==========================================
    # 3. Truncate Tables Idempotently via JVM DDL
    # ==========================================
    try:
        print("Truncating existing DWH tables to ensure clean, idempotent reload...")
        sc = spark.sparkContext
        jvm = sc._jvm
        
        # Access Context ClassLoader where the dynamically loaded Postgres Driver jar resides
        ctx_cl = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        
        # Load and instantiate the Driver class reflectively using Java Reflection
        driver_class = ctx_cl.loadClass("org.postgresql.Driver")
        driver_instance = driver_class.newInstance()
        
        # Construct java.util.Properties for the direct connection
        props = jvm.java.util.Properties()
        props.setProperty("user", db_properties["user"])
        props.setProperty("password", db_properties["password"])
        
        # Connect directly using the driver instance to bypass DriverManager classloader isolation
        conn = driver_instance.connect(db_url, props)
        if conn is None:
            raise Exception("driver_instance.connect returned null! The driver did not accept the URL.")
            
        stmt = conn.createStatement()
        stmt.execute("TRUNCATE TABLE fact_sales_transactions, dim_customers, dim_products, dim_sellers, dim_reviews, dim_time CASCADE;")
        stmt.close()
        conn.close()
        print("Database tables truncated successfully (CASCADE). DWH is clean.")
    except Exception as e:
        print(f"Warning: Cascade table truncation failed: {e}. Attempting DWH load anyway...")

    # ==========================================
    # 4. Load Data in Strict Integrous Sequence
    # ==========================================
    print("Beginning sequential loading process...")
    
    # Group dimensions for sequential loading
    dimensions = [
        (df_customers, "dim_customers"),
        (df_products, "dim_products"),
        (df_sellers, "dim_sellers"),
        (df_reviews, "dim_reviews"),
        (df_time, "dim_time")
    ]
    
    # 4.1 Load Dimensions First (in append mode now that DB is fully truncated)
    for df, name in dimensions:
        success = load_table_to_postgres(df, name, db_url, db_properties)
        if not success:
            print(f"CRITICAL ERROR: Failed to load dimension {name}. Aborting loading pipeline to prevent constraint violations.")
            spark.stop()
            sys.exit(1)
            
    # 4.2 Load Fact Table Last
    print("All dimensions loaded successfully. Proceeding to load fact table...")
    success = load_table_to_postgres(df_fact, "fact_sales_transactions", db_url, db_properties)
    if not success:
        print("CRITICAL ERROR: Failed to load fact table fact_sales_transactions.")
        spark.stop()
        sys.exit(1)
        
    print("=======================================================================")
    print("ALL TRANSACTIONS AND DIMENSIONS SUCCESSFULLY LOADED INTO POSTGRES DWH!")
    print("=======================================================================")
    
    spark.stop()

if __name__ == '__main__':
    main()
