import os
import json
import random
import uuid
import logging
from datetime import datetime
from confluent_kafka import Producer
from time import sleep

# Load environment variables
bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
topic_name = os.environ.get('INPUT_TOPIC')

# Define possible interaction types and items
interaction_types = ['click', 'view', 'purchase']
item_ids = range(1, 1001)  # Simulating 1000 unique items
user_ids = range(1000, 999999)  # Simulating 1000 unique items

# Generate random interaction data
def generate_interaction():
    user_id = random.choice(user_ids)  # Unique user ID
    item_id = random.choice(item_ids)  # Random item
    interaction_type = random.choice(interaction_types)  # Random interaction type
    timestamp = datetime.now().isoformat()  # Current UTC time in ISO format

    return {
        'user_id': user_id,
        'item_id': item_id,
        'interaction_type': interaction_type,
        'timestamp': timestamp
    }

# Callback function for message delivery
def delivery_report(err, msg):
    if err:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()}')

def main():
    logging.basicConfig(level=logging.INFO)

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': bootstrap_servers
    }
    producer = Producer(producer_config)

    # Infinite loop to generate and send messages
    while True:
        sending_data = generate_interaction()
        producer.produce(topic_name, value=json.dumps(sending_data).encode('utf-8'), callback=delivery_report)
        producer.poll(5)  # Poll for delivery reports
        logging.info('Sending interaction data...')
        sleep(5)

if __name__ == '__main__':
    main()
