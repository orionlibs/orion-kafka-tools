package io.github.orionlibs.orion_kafka_tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orionlibs.orion_kafka_tools.embedded.KafkaConsumer;
import io.github.orionlibs.orion_kafka_tools.embedded.KafkaProducer;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class EmbeddedKafkaIntegrationTest
{
    @Autowired
    public KafkaTemplate<String, String> template;
    @Autowired
    private KafkaConsumer consumer;
    @Autowired
    private KafkaProducer producer;
    @Value("${test.topic}")
    private String topic;
    private ObjectMapper objectMapper = new ObjectMapper();


    @BeforeEach
    void setup()
    {
        consumer.resetLatch();
    }


    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithDefaultTemplate_thenMessageReceived() throws Exception
    {
        String data = "Sending with default template";
        template.send(topic, data);
        boolean messageConsumed = consumer.getLatch()
                        .await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);
        assertTrue(consumer.getPayload().contains(data));
    }


    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived() throws Exception
    {
        String data = "Sending with our own simple KafkaProducer";
        producer.send(topic, data);
        boolean messageConsumed = consumer.getLatch()
                        .await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);
        assertTrue(consumer.getPayload().contains(data));
    }
}
