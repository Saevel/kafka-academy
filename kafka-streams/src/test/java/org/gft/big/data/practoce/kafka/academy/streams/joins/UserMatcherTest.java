package org.gft.big.data.practoce.kafka.academy.streams.joins;

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
import org.gft.big.data.practice.kafka.academy.streams.joins.UserMatcher;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class UserMatcherTest {

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
            Generators.streamOf(userGenerator).map(stream -> stream.limit(50).collect(Collectors.toList()));

    private ObjectMapper mapper = new ObjectMapper();

    private UserMatcher matcher = new UserMatcher();

    private ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(inputTopic, new StringSerializer(), new StringSerializer());

    @Test
    public void testUserMatcher() throws IOException {

        List<User> users = usersGenerator.sample();

        StreamsBuilder builder = new StreamsBuilder();

        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "NameHistogramCalculatorTest");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KStream<String, String> initialStream = builder.stream(inputTopic);

        KStream<String, User> userStream = initialStream.mapValues(value -> {
            try {
                return mapper.readValue(value, User.class);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        matcher.matchUsers(userStream, userStream, 10 * 1000)
                .mapValues(pair -> pair.toString())
                .to(outputTopic);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfig);

        for(User user : users) {
            try {
                testDriver.pipeInput(recordFactory.create(inputTopic, Long.toString(user.getId()), mapper.writeValueAsString(user)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        ProducerRecord<String, String> output = null;
        do {
            output = testDriver.readOutput(outputTopic, new StringDeserializer(), new StringDeserializer());
            if(output != null) {
                String[] outputData = output.value().replaceAll("\\(", "").replaceAll("\\)", "").split(", ");
                Assert.assertTrue(outputData.length == 2);

                User first = mapper.readValue(outputData[0], User.class);
                User second = mapper.readValue(outputData[1], User.class);

                Assert.assertTrue(first.getName() == second.getName());

            }
        } while (output != null);

        testDriver.close();
    }
}
