from confluent_kafka import Producer
import socket

class UserProducer:

    def produce(self, bootstrap_servers, topic, users):

        print("topic: " + topic)

        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            # For new versions of Mac OS to avoid the IPV6 death trap
            'broker.address.family': "v4"
        }

        producer = Producer(conf)

        for user in users:
            print("User produced: " + str(user))
            producer.produce(topic, key="key", value=str(user))

        producer.flush()