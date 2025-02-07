import json
import sqlite3
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import time

# Kafka Consumer Configuration
KAFKA_TOPIC = 'smart-home-topic'
KAFKA_BROKER = 'localhost:9092'

# Set up logging for better error tracking and debugging
logging.basicConfig(level=logging.INFO)

# SQLite Setup: Create a database connection
def create_db_connection():
    try:
        conn = sqlite3.connect('alerts.db')
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
        return None

# Create alert table if it doesn't exist
def create_alert_table():
    conn = create_db_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS temperature_alerts
                          (id INTEGER PRIMARY KEY, timestamp TEXT, temperature REAL, alert_type TEXT)''')
        conn.commit()
        conn.close()
        logging.info("Alert table created or already exists.")
    else:
        logging.error("Failed to create table due to DB connection issue.")

# Create Kafka Consumer
def create_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            group_id='smart-home-group',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Start consuming from the earliest message if no offset is available
        )
        logging.info("Kafka consumer connected.")
        return consumer
    except NoBrokersAvailable as e:
        logging.error(f"Kafka connection failed: {e}")
        raise
    except KafkaError as e:
        logging.error(f"Kafka error occurred: {e}")
        raise

# Process each message and insert alert if necessary
def process_message(message):
    timestamp = message.get("timestamp")
    temperature = float(message.get("temperature", 0.0))

    # Check if temperature exceeds threshold
    if temperature > 25.0:  # Temperature threshold for alert
        conn = create_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO temperature_alerts (timestamp, temperature, alert_type) VALUES (?, ?, ?)",
                (timestamp, temperature, 'Temperature Alert')
            )
            conn.commit()
            conn.close()
            logging.info(f"Temperature Alert: {temperature}°C at {timestamp}")
        else:
            logging.error("Failed to insert alert due to database connection issue.")

# Consume and process Kafka messages
def consume_messages():
    consumer = create_consumer()
    while True:
        try:
            # Keep listening to Kafka for new messages
            for message in consumer:
                process_message(message.value)
        except Exception as e:
            logging.error(f"Error while consuming messages: {e}")
            time.sleep(5)  # Wait before trying again

if __name__ == "__main__":
    # Create the alert table on initial run
    create_alert_table()

    # Start consuming messages
    try:
        consume_messages()
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
