package io.github.orionlibs.orion_kafka_tools.embedded;

import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer
{
    private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class.getName());
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void send(String topic, String payload)
    {
        LOGGER.info("sending payload='" + payload + "' to topic='" + topic + "'");
        kafkaTemplate.send(topic, payload);
    }
}
