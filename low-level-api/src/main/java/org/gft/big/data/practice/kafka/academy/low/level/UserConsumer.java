package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gft.big.data.practice.kafka.academy.model.User;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

public class UserConsumer {

    private ObjectMapper mapper;

    private Duration pollingTimeout;

    public UserConsumer(ObjectMapper mapper, Duration pollingTimeout) {
        this.mapper = mapper;
        this.pollingTimeout = pollingTimeout;
    }

    public List<User> consumeUsers(String bootstrapServers, String topic, String groupId, Duration timeout, long maxRecords){
        return null;
    }
}
