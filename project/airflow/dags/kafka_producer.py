from confluent_kafka import Producer
from config import BROKER

def get_producer():
    return Producer({'bootstrap.servers': BROKER})

def send_to_kafka(topic, data):
    producer = get_producer()
    producer.produce(topic, data)
    producer.flush()