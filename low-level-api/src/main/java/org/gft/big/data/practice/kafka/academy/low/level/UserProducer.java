package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikhalskiy.futurity.Futurity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gft.big.data.practice.kafka.academy.model.User;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserProducer class and
 * implement the produceUsers method.
 *
 * To serialize each User to JSON (use the ObjectMapper to achieve this),
 * Send it to Kafka under the user's id and return a single CompletableFuture which finishes
 * if / when all the user records are sent to Kafka,
 *
 * Use the Futurity.shift method to transform a simple Java Future to a CompletableFuture
 * Once implemented, run the UserProducerTest to check the correctness of your implementation.
 */
public class UserProducer {

    private ObjectMapper objectMapper;

    public UserProducer(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<?> produceUsers(String bootstrapServers, String topic, Collection<User> users){
        return null;
    }
}
