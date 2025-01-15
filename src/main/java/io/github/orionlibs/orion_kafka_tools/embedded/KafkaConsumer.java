package io.github.orionlibs.orion_kafka_tools.embedded;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer
{
    private static final Logger LOGGER = Logger.getLogger(KafkaConsumer.class.getName());
    private CountDownLatch latch = new CountDownLatch(1);
    private String payload;


    @KafkaListener(topics = "${test.topic}", groupId = "${test.groupid}")
    public void receive(ConsumerRecord<?, ?> consumerRecord)
    {
        LOGGER.info("received payload='" + consumerRecord.toString() + "'");
        payload = consumerRecord.toString();
        latch.countDown();
    }


    public CountDownLatch getLatch()
    {
        return latch;
    }


    public void resetLatch()
    {
        latch = new CountDownLatch(1);
    }


    public String getPayload()
    {
        return payload;
    }
}
