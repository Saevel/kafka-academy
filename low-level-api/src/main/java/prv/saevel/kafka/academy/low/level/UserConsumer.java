package prv.saevel.kafka.academy.low.level;

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

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserConsumer class and implement
 * the consumeUsers method so that it continuously reads the records from a given Kafka topic
 * until the maxRecords record count or the timeout is reached and then deserializes each value
 * to a User class from JSON (use the ObjectMapper) to eventually return the retrieved users.
 *
 * One implemented, run the UserConsumerTest to check the correctness of your implementation
 */
public class UserConsumer {

    private ObjectMapper mapper;

    private Duration pollingTimeout;

    public UserConsumer(ObjectMapper mapper, Duration pollingTimeout) {
        this.mapper = mapper;
        this.pollingTimeout = pollingTimeout;
    }

    public List<User> consumeUsers(String bootstrapServers, String topic, String groupId, Duration timeout, long maxRecords){

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerConfigs);
        consumer.subscribe(Arrays.asList(topic));

        List<User> users = new LinkedList<>();
        LocalDateTime startTimestamp = LocalDateTime.now();
        while(Duration.between(startTimestamp, LocalDateTime.now()).compareTo(timeout) <= 0 && users.size() <= maxRecords){
            consumer.poll(pollingTimeout).records(topic).forEach(record -> {
                try {
                    users.add(mapper.readValue(record.value(), User.class));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        return users;
    }
}