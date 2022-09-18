package prv.saevel.kafka.academy.low.level;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@RunWith(JUnit4.class)
public class UserProducerTest {

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

    private UserProducer producer = new UserProducer(mapper);

    @Test
    public void userProducerTest() throws InterruptedException, ExecutionException, TimeoutException {
        List<User> users = usersGenerator.sample();

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.getEmbeddedKafka().getBrokersAsString());
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerConfigs.put("broker.address.family", "v4");


        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerConfigs);

        producer.sendToKafka(rule.getEmbeddedKafka().getBrokersAsString(), topicName, users)
                .get(totalTimeout.toMinutes(), TimeUnit.MINUTES);

        rule.getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);

        List<String> stringifiedRecords = new LinkedList<>();
        KafkaTestUtils.getRecords(consumer).forEach(keyValue -> stringifiedRecords.add(keyValue.value()));

        List<User> retrievedUsers = stringifiedRecords.stream().map(s -> {
            try {
                return mapper.readValue(s, User.class);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toList());

        assertThat(retrievedUsers, containsInAnyOrder(users.toArray()));
    }
}
