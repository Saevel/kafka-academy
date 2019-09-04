package org.gft.big.data.practoce.kafka.academy.streams.aggregations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.streams.GenericSerde;
import org.gft.big.data.practice.kafka.academy.streams.aggregations.NameHistogramCalculator;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NameHistogramCalculatorTest {

    private String inputTopic = "input-topic";

    private String outputTopic = "output-topic";

    private String bootstrapServers = "dummy://1234";

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

    private NameHistogramCalculator calculator = new NameHistogramCalculator();

    private GenericSerde<String> stringSerde = new GenericSerde<>();

    private GenericSerde<Long> longSerde = new GenericSerde<>();

    private GenericSerde<User> userSerde = new GenericSerde<>();

    private ConsumerRecordFactory<Long, User> recordFactory =
            new ConsumerRecordFactory<>(inputTopic, longSerde.serializer(), userSerde.serializer());

    @Test
    public void nameHistogramCalculatorTest(){

        List<User> users = usersGenerator.sample();
        Map<String, Long> expectedHistograms = calculateHistograms(users);

        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue> keyValues = users
                .stream()
                .map(user -> new KeyValue(user.getId(), user))
                .collect(Collectors.toList());

        /*
         * KLUDGE: Closing the Test Driver doesn't close the state stores properly. Changing applicationId will work
         * around this, but will bloat your tmp folder, so be sure to delete it after some runs.
         */
        Random random = new Random();
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "NameHistogramCalculatorTest" + random.nextInt());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericSerde.class.getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericSerde.class.getName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KStream<Long, User> userStream = builder.stream(inputTopic);

        calculator.calculateNameHistograms(userStream)
                .toStream()
                .to(outputTopic);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfig);

        for(KeyValue<Long, User> keyValue : keyValues) {
            testDriver.pipeInput(recordFactory.create(inputTopic, keyValue.key, keyValue.value));
        }

        ProducerRecord<String, Long> output;
        do {
            output = testDriver.readOutput(outputTopic, stringSerde.deserializer(), longSerde.deserializer());
            if(output != null) {
                long expectedValue = expectedHistograms.get(output.key());
                long actualValue = output.value();
                Assert.assertTrue(
                        "The histogram values do not agree. Expected maximal value: " + expectedValue + ". Actual value: " + actualValue,
                        actualValue <= expectedValue
                );
            }
        } while (output != null);
    }

    private Map<String, Long> calculateHistograms(List<User> users){
        return users
                .stream()
                .collect(Collectors.toMap((user) -> user.getName(), (user) -> 1L, (i, j) -> i + j, () -> new HashMap<>()));
    }
}
