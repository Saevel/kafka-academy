from confluent_kafka import Producer
import socket
import pytest
import sys
import json
import os

from main.UserConsumer import UserConsumer

def test_properly_consuming_users():
    kafka_bootstrap_servers = os.environ['kafka.bootstrap.servers']

    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'client.id': socket.gethostname()
    }

    print("Kafka config: " + str(conf))

    expected_data = json.load(open("users.json"))

    print("Expected data: " + str(expected_data))

    producer = Producer(conf)

    for i in expected_data:
        producer.produce("users", key="key", value=str(expected_data))
        producer.flush()

    user_consumer = UserConsumer()
    actual_users = user_consumer.consume(kafka_bootstrap_servers, "users-consumer", 100, len(expected_data), 60*1000)

    print("Actual data: " + str(actual_users))

    assert expected_data == actual_users
    assert 1 == 1
