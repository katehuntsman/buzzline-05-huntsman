import json
import random
import time
from kafka import KafkaProducer
import uuid

# Kafka Producer Configuration
KAFKA_TOPIC = 'smart-home-topic'
KAFKA_BROKER = 'localhost:9092'

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    """Generates simulated sensor data for the smart home system."""
    return {
        "sensor_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "temperature": round(random.uniform(18.0, 30.0), 2),  # temperature in Celsius
        "humidity": round(random.uniform(30.0, 70.0), 2),     # humidity percentage
        "motion_detected": random.choice([True, False]),       # motion detection status
    }

def produce_data():
    """Produce sensor data and send to Kafka"""
    while True:
        sensor_data = generate_sensor_data()
        producer.send(KAFKA_TOPIC, sensor_data)
        print(f"Sent data: {sensor_data}")
        time.sleep(1)  # Simulating sensor data production every second

if __name__ == '__main__':
    produce_data()
