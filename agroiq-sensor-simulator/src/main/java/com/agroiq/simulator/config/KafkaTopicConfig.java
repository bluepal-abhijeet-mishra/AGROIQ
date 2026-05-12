package com.agroiq.simulator.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String RAW_SENSORS_TOPIC = "farm.sensors.raw";
    public static final String SIMULATOR_COMMANDS_TOPIC = "farm-simulator-commands";
    public static final String WEATHER_DATA_TOPIC = "weather.data";
    private static final String THREE_DAYS_MS = "259200000";
    private static final String SEVEN_DAYS_MS = "604800000";

    @Bean
    public NewTopic rawSensorsTopic() {
        return TopicBuilder.name(RAW_SENSORS_TOPIC)
                .partitions(12)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, THREE_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic simulatorCommandsTopic() {
        return TopicBuilder.name(SIMULATOR_COMMANDS_TOPIC)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, SEVEN_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic weatherDataTopic() {
        return TopicBuilder.name(WEATHER_DATA_TOPIC)
                .partitions(6)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, SEVEN_DAYS_MS)
                .build();
    }
}
