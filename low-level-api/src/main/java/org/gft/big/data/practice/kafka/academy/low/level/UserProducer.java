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

public class UserProducer {

    private ObjectMapper objectMapper;

    public UserProducer(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<?> sendToKafka(String bootstrapServers, String topic, Collection<User> users){
        return null;
    }
}
