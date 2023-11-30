import os


def get_kafka_url():
    server_url = os.getenv('X-Kafka-Url')
    return server_url
