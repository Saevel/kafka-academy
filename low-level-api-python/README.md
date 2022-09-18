# Low Level API in Python

## Task 1: Producer API

Go to the "UserProducer.py" file and implement the "produce" method to serialize each "User" JSON to String, send it to
Kafka under any key and only terminate once all the users have been sent. Use the "confluent-kafka" library to achieve
that.

To create a virtual environment inside the project, call the "createVirtualEnvironment" Gradle command. Then, to
active it call the "venv/Scripts/activate" script.

Once implemented, run the "testAll" Gradle command to check the correctness of your implementation. The tests in
"test_user_producer.py" file should now pass.


## Task 2: Consumer API

Go to the "UserConsumer.py" file and implement the "consume" method so that it continuously reads the records from a
given Kafka topic until the "max_records" record count or the "total_timeout_ms" is reached and then deserializes each
value to a JSON map to eventually return the retrieved users.

To create a virtual environment inside the project, call the "createVirtualEnvironment" Gradle command. Then, to
active it call the "venv/Scripts/activate" script.

Once implemented, run the "testAll" Gradle command to check the correctness of your implementation. The tests in the
"test_user_consumer.py" file should now pass.
