package com.agroiq.simulator.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String RAW_SENSORS_TOPIC = "farm.sensors.raw";

    @Bean
    public NewTopic rawSensorsTopic() {
        return TopicBuilder.name(RAW_SENSORS_TOPIC)
                .partitions(12)
                .replicas(1)
                .build();
    }
}
