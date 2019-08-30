Low Level API

Task 1: Producer API

Go to the "org.gft.big.data.practice.kafka.academy.low.level.UserProducer" class and implement the "produceUsers"
method to serialize each "User" to JSON (use the ObjectMapper to achieve this), send it to Kafka under the user's "id"
and return a single CompletableFuture which finishes if / when all the user records are sent to Kafka. Use the 
"Futurity.shift" method to transform a simple Java Future to a CompletableFuture. 

Once implemented, run the "UserProducerTest" to check the correctness of your implementation.


Task 2: Consumer API

Go to the "org.gft.big.data.practice.kafka.academy.low.level.UserConsumer" class and implement the "consumeUsers" method
so that it continuously reads the records from a given Kafka topic until the "maxRecords" record count or the "timeout" 
is reached and then deserializes each value to a "User" class from JSON (use the ObjectMapper) to eventually return the 
retrieved users.

One implemented, run the "UserConsumerTest" to check the correctness of your implementation.
