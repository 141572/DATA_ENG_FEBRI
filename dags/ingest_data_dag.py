import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add /workspace/scripts to Python path so we can import kafka_producer
sys.path.append('/workspace')
from scripts.kafka_producer import publish_csv_to_kafka

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

def run_producer(csv_filename, topic_name):
    """
    Wrapper function to run the publish_csv_to_kafka producer inside a PythonOperator.
    """
    workspace_path = '/workspace'
    csv_file_path = os.path.join(workspace_path, 'dataset', csv_filename)
    bootstrap_servers = 'kafka:29092' # Internal Docker host
    
    print(f"Starting ingestion of {csv_filename} into topic {topic_name}...")
    success = publish_csv_to_kafka(
        csv_file_path=csv_file_path,
        kafka_topic=topic_name,
        bootstrap_servers=bootstrap_servers
    )
    if not success:
        raise Exception(f"Ingestion failed for file: {csv_filename}")
    print(f"Ingestion successful for {csv_filename}!")

# Define the DAG
with DAG(
    'olist_data_ingestion',
    default_args=default_args,
    description='Orchestrated batch ingestion of Olist CSV files into Apache Kafka Topics',
    schedule_interval=None, # Trigger manually from Web UI
    catchup=False,
    tags=['e-commerce', 'olist', 'ingestion', 'kafka']
) as dag:

    # 1. Dimension Tables Ingestion Tasks (Can run in parallel)
    ingest_customers = PythonOperator(
        task_id='ingest_customers',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_customers_dataset.csv',
            'topic_name': 'olist_customers'
        }
    )

    ingest_products = PythonOperator(
        task_id='ingest_products',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_products_dataset.csv',
            'topic_name': 'olist_products'
        }
    )

    ingest_sellers = PythonOperator(
        task_id='ingest_sellers',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_sellers_dataset.csv',
            'topic_name': 'olist_sellers'
        }
    )

    ingest_geolocation = PythonOperator(
        task_id='ingest_geolocation',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_geolocation_dataset.csv',
            'topic_name': 'olist_geolocation'
        }
    )

    ingest_category_translation = PythonOperator(
        task_id='ingest_category_translation',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'product_category_name_translation.csv',
            'topic_name': 'product_category_translation'
        }
    )

    # 2. Orders Ingestion (Depends on dimensions)
    ingest_orders = PythonOperator(
        task_id='ingest_orders',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_orders_dataset.csv',
            'topic_name': 'olist_orders'
        }
    )

    # 3. Transaction Details (Depends on Orders)
    ingest_order_items = PythonOperator(
        task_id='ingest_order_items',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_order_items_dataset.csv',
            'topic_name': 'olist_order_items'
        }
    )

    ingest_order_payments = PythonOperator(
        task_id='ingest_order_payments',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_order_payments_dataset.csv',
            'topic_name': 'olist_order_payments'
        }
    )

    ingest_order_reviews = PythonOperator(
        task_id='ingest_order_reviews',
        python_callable=run_producer,
        op_kwargs={
            'csv_filename': 'olist_order_reviews_dataset.csv',
            'topic_name': 'olist_order_reviews'
        }
    )

    # Define DAG Orchestration Flow Structure
    # Dimensions >> Primary Transactions >> Transaction Details & Details
    [
        ingest_customers,
        ingest_products,
        ingest_sellers,
        ingest_geolocation,
        ingest_category_translation
    ] >> ingest_orders >> [
        ingest_order_items,
        ingest_order_payments,
        ingest_order_reviews
    ]
