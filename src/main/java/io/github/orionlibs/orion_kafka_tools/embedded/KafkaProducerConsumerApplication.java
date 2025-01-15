package io.github.orionlibs.orion_kafka_tools.embedded;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerConsumerApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(KafkaProducerConsumerApplication.class, args);
    }
}
