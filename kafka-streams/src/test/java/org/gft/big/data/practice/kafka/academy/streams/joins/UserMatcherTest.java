package org.gft.big.data.practice.kafka.academy.streams.joins;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.streams.GenericSerde;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Random;
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

    private UserMatcher matcher = new UserMatcher();

    private GenericSerde<Long> longSerde = new GenericSerde<>();

    private GenericSerde<User> userSerde = new GenericSerde<>();

    private ConsumerRecordFactory<Long, User> recordFactory =
            new ConsumerRecordFactory<>(inputTopic, longSerde.serializer(), userSerde.serializer());

    @Test
    public void testUserMatcher() {

        List<User> users = usersGenerator.sample();

        StreamsBuilder builder = new StreamsBuilder();

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

        matcher.matchUsers(userStream, userStream, 10 * 1000)
                .to(outputTopic);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfig);

        for(User user : users) {
            testDriver.pipeInput(recordFactory.create(inputTopic, user.getId(), user));
        }

        ProducerRecord<User, User> output;
        do {
            output = testDriver.readOutput(outputTopic, userSerde.deserializer(), userSerde.deserializer());
            if(output != null) {

                User first = output.key();
                User second = output.value();

                Assert.assertEquals("Users do not match by surname. First user: " + first + ". Second user: " + second,
                    first.getSurname(), second.getSurname());
            }
        } while (output != null);
    }
}
