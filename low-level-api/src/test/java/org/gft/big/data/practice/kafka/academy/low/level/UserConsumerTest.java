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
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class UserConsumerTest {

    private Duration totalTimeout = Duration.ofMinutes(3);

    private static final String topicName = "TestTopic";

    private String consumerGroup = "sample";

    @ClassRule
    public static EmbeddedKafkaRule rule = new EmbeddedKafkaRule(1, false, topicName);

    private Generator<User> userGenerator = Generators.longGenerator().flatMap(userId ->
            Generators.oneOf("Kamil", "Artur", "Leszek", "Iwona", "Elżbieta", "Andrzej").flatMap(name ->
                    Generators.oneOf("Owczarek", "Mazur", "Wołyniec").flatMap(surname ->
                            Generators.range(1, 100).map(age ->
                                    new User(userId, name, surname, age)
                            )
                    )
            )
    );

    private Generator<List<User>> usersGenerator =
            Generators.streamOf(userGenerator).map(stream -> stream.limit(10).collect(Collectors.toList()));

    private ObjectMapper mapper = new ObjectMapper();

    private UserConsumer userConsumer = new UserConsumer(mapper, Duration.ofMillis(100));

    @Test
    public void setUserConsumerTest() throws InterruptedException, ExecutionException, TimeoutException {

        List<User> users = usersGenerator.sample();

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.getEmbeddedKafka().getBrokersAsString());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Long, String> producer = new KafkaProducer<>(configs);

        users.stream()
                .map(value -> {
                    try {
                        String stringBody = mapper.writeValueAsString(value);
                        return new ProducerRecord<Long, String>(topicName, value.getId(), stringBody);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
        }).map(producer::send)
                .map(Futurity::shift)
                .reduce((left, right) -> left.thenCompose(any -> right))
                .get()
                .get(totalTimeout.toMillis(), TimeUnit.MILLISECONDS);

        List<User> retrievedUsers = userConsumer.consumeUsers(rule.getEmbeddedKafka().getBrokersAsString(), topicName, "random", Duration.ofSeconds(10, 10), users.size());

        assertThat(retrievedUsers, containsInAnyOrder(users.toArray()));
    }
}
