import socket
import json
import time

from confluent_kafka import Consumer


class UserConsumer:

    def consume(self, bootstrap_servers, topic, poll_timeout_seconds, max_records, total_timeout_seconds):
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'auto.offset.reset': "earliest",
            "group.id": "dsfaggrgg"
        }

        print("Kafka config: " + str(conf))

        consumer = Consumer(conf)

        results = []

        start_time = time.time()
        try:
            consumer.subscribe([topic])

            while (len(results) < max_records) and ((time.time() - start_time) < total_timeout_seconds) :
                msg = consumer.poll(timeout = poll_timeout_seconds)
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
                    results.append(json.loads(msg.value().decode('utf8').replace("'", '"')))
        except Exception as e:
            print("Error occurred: " + str(e))
        finally:
            consumer.close()
        
        return results