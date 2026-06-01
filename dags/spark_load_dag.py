from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def log_load_instructions():
    """Logs instructions for manual Spark database loading."""
    print("=======================================================================")
    print("APACHE SPARK POSTGRESQL DATABASE DWH LOADER")
    print("=======================================================================")
    print("Untuk menjalankan proses loading dari penyimpanan Parquet ke dalam")
    print("database PostgreSQL Data Warehouse Anda, silakan jalankan perintah")
    print("berikut di terminal/PowerShell host lokal Anda:")
    print("")
    print("docker exec -it spark-master spark-submit \\")
    print("  --packages org.postgresql:postgresql:42.6.0 \\")
    print("  /workspace/scripts/spark_load.py")
    print("")
    print("Perintah di atas akan:")
    print("1. Mengunduh driver PostgreSQL JDBC secara otomatis.")
    print("2. Membaca 6 tabel Parquet bersih dari folder /workspace/tmp_transformed/.")
    print("3. Memuat dimensi (customers, products, sellers, reviews, time) terlebih dahulu.")
    print("4. Memuat tabel fakta (fact_sales_transactions) terakhir secara aman.")
    print("=======================================================================")

with DAG(
    'olist_spark_load',
    default_args=default_args,
    description='Orchestrates and documents the PySpark PostgreSQL DWH Loading Job',
    schedule_interval=None,
    catchup=False,
    tags=['e-commerce', 'olist', 'loading', 'spark', 'postgres']
) as dag:

    # Task to print Spark Submit command and instructions
    display_instructions = PythonOperator(
        task_id='display_load_instructions',
        python_callable=log_load_instructions
    )
    
    # Optional task: attempts to run a local notification
    load_complete = BashOperator(
        task_id='spark_load_complete_signal',
        bash_command='echo "PySpark loading process instructions have been logged. Please check task logs and execute the spark-submit command on host terminal."'
    )

    display_instructions >> load_complete
