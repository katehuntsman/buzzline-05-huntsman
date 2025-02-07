import json
import sqlite3
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import time

# Kafka Consumer Configuration
KAFKA_TOPIC = 'smart-home-topic'
KAFKA_BROKER = 'localhost:9092'

# SQLite Setup
def create_db_connection():
    return sqlite3.connect('alerts.db')

def create_alert_table():
    conn = create_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS temperature_alerts
                      (id INTEGER PRIMARY KEY, timestamp TEXT, temperature REAL, alert_type TEXT)''')
    conn.commit()
    conn.close()

create_alert_table()

# Kafka Consumer Setup
def create_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            group_id='smart-home-group',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logging.info("Kafka consumer connected.")
        return consumer
    except NoBrokersAvailable as e:
        logging.error(f"Kafka connection failed: {e}")
        raise

# Process each message and insert alert if necessary
def process_message(message):
    timestamp = message.get("timestamp")
    temperature = float(message.get("temperature", 0.0))

    if temperature > 30.0:  # Temperature threshold for alert (updated to 30°C)
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO temperature_alerts (timestamp, temperature, alert_type) VALUES (?, ?, ?)",
                       (timestamp, temperature, 'Temperature Alert'))
        conn.commit()
        conn.close()
        logging.info(f"Temperature Alert: {temperature}°C at {timestamp}")

# Consume and process Kafka messages
def consume_messages():
    consumer = create_consumer()
    for message in consumer:
        process_message(message.value)

if __name__ == "__main__":
    consume_messages()
