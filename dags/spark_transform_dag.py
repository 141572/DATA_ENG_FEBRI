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

def log_spark_instructions():
    """Logs instructions for manual Spark execution."""
    print("=======================================================================")
    print("APACHE SPARK DISTRIBUTED TRANSFORMATION ENGINE")
    print("=======================================================================")
    print("Untuk menjalankan transformasi PySpark secara terdistribusi di dalam")
    print("cluster Spark Master & Spark Worker Anda, silakan jalankan perintah")
    print("berikut di terminal/PowerShell host lokal Anda:")
    print("")
    print("docker exec -it spark-master spark-submit \\")
    print("  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\")
    print("  /workspace/scripts/spark_transform.py")
    print("")
    print("Perintah di atas akan:")
    print("1. Mengunduh driver integrasi Kafka secara otomatis.")
    print("2. Menghubungkan ke broker Kafka dan mengkonsumsi seluruh topik JSON.")
    print("3. Memproses pembersihan data, data quality checks, dan feature engineering.")
    print("4. Menyimpan output terbersih ke dalam folder /workspace/tmp_transformed/.")
    print("=======================================================================")

with DAG(
    'olist_spark_transform',
    default_args=default_args,
    description='Orchestrates and documents the PySpark Star Schema Transformation Job',
    schedule_interval=None, # Trigger manually or after ingestion
    catchup=False,
    tags=['e-commerce', 'olist', 'transformation', 'spark']
) as dag:

    # Task to print Spark Submit commands and instructions
    display_instructions = PythonOperator(
        task_id='display_spark_instructions',
        python_callable=log_spark_instructions
    )
    
    # Optional task: attempts to run a local notification or trigger (can be extended in production)
    transform_complete = BashOperator(
        task_id='spark_transform_complete_signal',
        bash_command='echo "PySpark distributed execution instructions have been logged. Please check task logs and execute the spark-submit command on host terminal."'
    )

    display_instructions >> transform_complete
