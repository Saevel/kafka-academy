package prv.saevel.kafka.academy;

import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class KafkaTestSimulation {

    private Optional<EmbeddedKafkaBroker> broker = Optional.empty();

    public Optional<String> start(int nodeCount, List<String> topics) {
        if(broker.isPresent()) {
            System.out.println("Broker is not shut down while starting the new one");
            return Optional.empty();
        } else {
            System.out.println("Starting Kafka brokers with node count: " + nodeCount + " and topics: " + topics);
            broker = Optional.of(new EmbeddedKafkaBroker(nodeCount, true, toArray(topics)));
            broker.ifPresent(EmbeddedKafkaBroker::afterPropertiesSet);

            String bootstrapServers = broker.map(EmbeddedKafkaBroker::getBrokersAsString).orElseThrow(() ->
                    new IllegalStateException("Failed to start Kafka brokers")
            );

            System.out.println("Started Kafka brokers on: " + bootstrapServers);
            return Optional.ofNullable(bootstrapServers);
        }
    }

    public void stop() throws java.io.IOException {
        if(broker.isPresent()) {
            broker.ifPresent(this::stopServer);
            broker = Optional.empty();
        } else {
            System.out.println("Cannot stop Kafka broker as it is not running");
        }
    }

    private void stopServer(EmbeddedKafkaBroker b) {
        System.out.println("Stopping Kafka brokers");
        b.getKafkaServers().stream().forEach(server -> server.shutdown());
        System.out.println("Kafka brokers stopped");
        try {
            System.out.println("Stopping Zookeeper server");
            b.getZookeeper().shutdown();
            System.out.println("Zookeeper server stopped");
        } catch(Exception e) {
            throw new IllegalStateException("Unable to shut down Zookeeper");
        }
    }

    private String[] toArray(List<String> list) {
        if(list == null || list.size() == 0) {
            return new String[0];
        } else {
            String[] result = new String[list.size()];
            for(int i = 0; i < list.size(); i++){
                result[i] = list.get(i);
            }
            return result;
        }
    }
}
