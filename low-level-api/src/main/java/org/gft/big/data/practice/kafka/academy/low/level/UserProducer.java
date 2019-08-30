package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikhalskiy.futurity.Futurity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

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

        Map<String, Object> producerSettings = new HashMap<>();
        producerSettings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerSettings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerSettings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Long, String> producer = new KafkaProducer<>(producerSettings);

        return users.stream()
                    .map(toProducerRecord(topic))
                    .map(producer::send)
                    .map(Futurity::shift)
                    .reduce((left, right) -> left.thenCompose(any -> right))
                    .get();
    }

    private Function<User, ProducerRecord<Long, String>> toProducerRecord(String topic){
        return user -> {
            try {
                return new ProducerRecord<>(topic, user.getId(), objectMapper.writeValueAsString(user));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        };
    }
}
