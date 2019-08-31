package org.gft.big.data.practoce.kafka.academy.streams.aggregations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.streams.aggregations.NameHistogramCalculator;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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

    private ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(inputTopic, new StringSerializer(), new StringSerializer());

    @Test
    public void nameHistogramCalculatorTest(){

        List<User> users = usersGenerator.sample();
        Map<String, Long> expectedHistograms = calculateHistograms(users);

        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<String, String>> keyValues = users
                .stream()
                .map(user -> {
                    try {
                        return new KeyValue<String, String>(Long.toString(user.getId()), mapper.writeValueAsString(user));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .collect(Collectors.toList());

        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "NameHistogramCalculatorTest");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KStream<String, String> initialStream = builder.stream(inputTopic);

        KStream<String, String> userStream = initialStream.mapValues(v -> {
            try {
                return mapper.readValue(v, User.class).getName();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
        }});

        calculator.calculateNameHistograms(userStream)
                .mapValues(l -> Long.toString(l))
                .toStream()
                .to(outputTopic);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfig);

        for(KeyValue<String, String> keyValue : keyValues) {
            testDriver.pipeInput(recordFactory.create(inputTopic, keyValue.key, keyValue.value));
        }

        ProducerRecord<String, String> output = null;
        do {
            output = testDriver.readOutput(outputTopic, new StringDeserializer(), new StringDeserializer());
            if(output != null) {
                Assert.assertTrue("The histogram values do not agree", (long)expectedHistograms.get(output.key()) >= Long.parseLong(output.value()));
            }
        } while (output != null);

        testDriver.close();
    }

    private Map<String, Long> calculateHistograms(List<User> users){
        return users
                .stream()
                .collect(Collectors.toMap((user) -> user.getName(), (user) -> 1L, (i, j) -> i + j, () -> new HashMap<>()));
    }
}
