import os
import socket
import json
import sys
import time

from confluent_kafka import Consumer

from main.UserProducer import UserProducer


def test_properly_producing_users():

    kafka_bootstrap_servers = os.environ['kafka.bootstrap.servers']

    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'client.id': socket.gethostname(),
        'auto.offset.reset': "earliest",
        "group.id": "fgfvsafgsfg",
        # For new versions of Mac OS to avoid the IPV6 death trap
        "broker.address.family": "v4"
    }

    print("Kafka config: " + str(conf))

    expected_data = json.load(open("users.json"))

    print("Expected data: " + str(expected_data))

    consumer = Consumer(conf)

    actual_results = []

    producer = UserProducer()
    producer.produce(kafka_bootstrap_servers, "users-producer", expected_data)

    start_time = time.time()
    try:
        consumer.subscribe(["users-producer"])

        while (len(actual_results) < len(expected_data)) and ((time.time() - start_time) < 10.0) :
            msg = consumer.poll(timeout = 1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                actual_results.append(json.loads(msg.value().decode('utf8').replace("'", '"')))
    except Exception as e:
        print("Error occurred: " + str(e))
    finally:
        consumer.close()

    assert expected_data == actual_results