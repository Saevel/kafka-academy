import socket
import json
import time

from confluent_kafka import Consumer


class UserConsumer:

    def consume(self, bootstrap_servers, topic, poll_timeout_seconds, max_records, total_timeout_seconds):
        return []