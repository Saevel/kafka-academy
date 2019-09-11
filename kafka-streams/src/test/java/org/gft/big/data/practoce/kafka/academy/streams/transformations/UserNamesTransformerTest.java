package org.gft.big.data.practoce.kafka.academy.streams.transformations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.streams.transformations.UserNamesTransformer;
import org.gft.big.data.practice.kafka.academy.tests.Generator;
import org.gft.big.data.practice.kafka.academy.tests.Generators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class UserNamesTransformerTest {

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

    private UserNamesTransformer transformer = new UserNamesTransformer();

    private ConsumerRecordFactory<Long, String> recordFactory =
            new ConsumerRecordFactory<>(inputTopic, new LongSerializer(), new StringSerializer());

    @Test
    public void transformerTest() throws InterruptedException, ExecutionException, TimeoutException {

        List<User> users = usersGenerator.sample();

        List<KeyValue<Long, String>> keyValues = users
                .stream()
                .map(user -> {
                    try {
                        return new KeyValue<>(user.getId(), mapper.writeValueAsString(user));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .collect(Collectors.toList());

        /*
         * KLUDGE: Closing the Test Driver doesn't close the state stores properly. Changing applicationId will work
         * around this, but will bloat your tmp folder, so be sure to delete it after some runs.
         */
        Random random = new Random();
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "UserNamesTransformerTest" + random.nextInt());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, String> initialStream = builder.stream(inputTopic);

        KStream<Long, User> userStream = initialStream.mapValues(v -> {
            try {
                return mapper.readValue(v, User.class);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });

        transformer.transform(userStream).to(outputTopic);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfig);

        for(KeyValue<Long, String> keyValue : keyValues) {
            testDriver.pipeInput(recordFactory.create(inputTopic, keyValue.key, keyValue.value));
        }

        ProducerRecord<Long, String> output;
        do {
            output = testDriver.readOutput(outputTopic, new LongDeserializer(), new StringDeserializer());
            if(output != null) {
                Assert.assertTrue("Each name must start with an A", output.value().startsWith("A"));
                Assert.assertEquals("Each entry must be name + pause + surname", 2, output.value().split(" ").length);
            }
        } while (output != null);
    }
}
