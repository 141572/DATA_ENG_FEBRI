import os
import csv
import json
import logging
import sys
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def on_send_success(record_metadata):
    """Callback for successful asynchronous send."""
    pass  # We can silence this in production to keep logs clean

def on_send_error(excp):
    """Callback for failed asynchronous send."""
    logger.error(f'Failed to send record to Kafka: {excp}')

def publish_csv_to_kafka(csv_file_path, kafka_topic, bootstrap_servers='kafka:29092', max_rows=None):
    """
    Reads records from a CSV file and publishes them as JSON messages to a Kafka topic.
    
    Args:
        csv_file_path (str): Path to the source CSV file.
        kafka_topic (str): Kafka topic name to send messages to.
        bootstrap_servers (str): Kafka broker bootstrap servers.
        max_rows (int, optional): Maximum number of rows to process (useful for testing).
    """
    if not os.path.exists(csv_file_path):
        logger.error(f"Source file not found: {csv_file_path}")
        return False

    logger.info(f"Connecting to Kafka broker at {bootstrap_servers}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',                 # Ensure strong data persistence
            retries=5,                  # Automatically retry failed sends
            linger_ms=10,               # Linger slightly for batching (high throughput)
            batch_size=16384 * 2        # 32KB batch size
        )
    except Exception as e:
        logger.error(f"Failed to initialize KafkaProducer: {e}")
        return False

    logger.info(f"Starting ingestion: [File: {os.path.basename(csv_file_path)}] -> [Topic: {kafka_topic}]")
    
    success_count = 0
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for i, row in enumerate(reader):
                if max_rows and i >= max_rows:
                    logger.info(f"Reached max row limit of {max_rows}. Stopping.")
                    break
                
                # Send asynchronously to Kafka
                producer.send(
                    kafka_topic, 
                    value=row
                ).add_callback(on_send_success).add_errback(on_send_error)
                
                success_count += 1
                
                # Log progress every 20,000 records
                if success_count % 20000 == 0:
                    logger.info(f"Ingested {success_count} records so far...")
                    
        # Flush the producer to guarantee all messages are sent before exiting
        logger.info("Flushing producer batches to broker...")
        producer.flush()
        logger.info(f"Ingestion completed successfully! Total records sent: {success_count}")
        producer.close()
        return True
        
    except Exception as e:
        logger.error(f"Error during ingestion process: {e}")
        if 'producer' in locals():
            producer.close()
        return False

if __name__ == '__main__':
    # Allow execution directly via CLI
    if len(sys.argv) < 3:
        print("Usage: python kafka_producer.py <csv_file_path> <kafka_topic> [bootstrap_servers] [max_rows]")
        sys.exit(1)
        
    csv_path = sys.argv[1]
    topic = sys.argv[2]
    servers = sys.argv[3] if len(sys.argv) > 3 else 'localhost:9092' # use localhost if running on host directly
    rows = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    publish_csv_to_kafka(csv_path, topic, bootstrap_servers=servers, max_rows=rows)
