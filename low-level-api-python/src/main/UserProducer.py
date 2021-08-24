from confluent_kafka import Producer
import socket

class UserProducer:

    def produce(self, bootstrap_servers, topic, users):

        print("topic: " + topic)

        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname()
        }

        producer = Producer(conf)

        for user in users:
            print("User produced: " + str(user))
            producer.produce(topic, key="key", value=str(user))

        producer.flush()