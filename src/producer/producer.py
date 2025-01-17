from confluent_kafka import Producer
from time import sleep
import logging
import pandas as pd
import json
import os
import random
import random
import time
import json
import uuid
from datetime import datetime

bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
topic_name = os.environ.get('INPUT_TOPIC')

# Define possible interaction types and items
interaction_types = ['click', 'view', 'purchase']
item_ids = range(1, 1001)  # Simulating 1000 unique items


def generate_interaction():
    user_id = str(uuid.uuid4())  # Unique user ID
    item_id = random.choice(item_ids)  # Random item
    interaction_type = random.choice(interaction_types)  # Random interaction type
    timestamp = f"{datetime.now()}"  # Current UTC time
    
    return {
        'user_id': item_id,
        'item_id': item_id,
        'interaction_type': interaction_type,
        'timestamp': timestamp
    }


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')

def main():
    logging.basicConfig(level=logging.INFO)
    producer_config = {
        'bootstrap.servers': bootstrap_servers    }
    producer = Producer(producer_config)
    while True:
        sending_data = generate_interaction()
        producer.produce(topic_name, value=json.dumps(sending_data).encode("utf-8"), callback=delivery_report)
        producer.poll(5)
        logging.info('Sending dataframe sample...')
        sleep(5)

if __name__ == '__main__':
    main()
    
