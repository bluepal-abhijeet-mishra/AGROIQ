package com.agroiq.simulator.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String RAW_SENSORS_TOPIC = "farm.sensors.raw";
    public static final String ENRICHED_SENSORS_TOPIC = "farm.sensors.enriched";
    public static final String SATELLITE_DATA_TOPIC = "satellite.data";
    public static final String CROP_RISK_ALERTS_TOPIC = "crop.risk.alerts";
    public static final String FARM_RECOMMENDATIONS_TOPIC = "farm.recommendations";
    public static final String ANALYTICS_METRICS_TOPIC = "analytics.metrics";
    public static final String SIMULATOR_COMMANDS_TOPIC = "farm-simulator-commands";
    public static final String WEATHER_DATA_TOPIC = "weather.data";
    private static final String THREE_DAYS_MS = "259200000";
    private static final String SEVEN_DAYS_MS = "604800000";
    private static final String THIRTY_DAYS_MS = "2592000000";
    private static final String NINETY_DAYS_MS = "7776000000";

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

    @Bean
    public NewTopic enrichedSensorsTopic() {
        return TopicBuilder.name(ENRICHED_SENSORS_TOPIC)
                .partitions(12)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, SEVEN_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic satelliteDataTopic() {
        return TopicBuilder.name(SATELLITE_DATA_TOPIC)
                .partitions(4)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, THIRTY_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic cropRiskAlertsTopic() {
        return TopicBuilder.name(CROP_RISK_ALERTS_TOPIC)
                .partitions(6)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, NINETY_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic farmRecommendationsTopic() {
        return TopicBuilder.name(FARM_RECOMMENDATIONS_TOPIC)
                .partitions(6)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, THIRTY_DAYS_MS)
                .build();
    }

    @Bean
    public NewTopic analyticsMetricsTopic() {
        return TopicBuilder.name(ANALYTICS_METRICS_TOPIC)
                .partitions(4)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, NINETY_DAYS_MS)
                .build();
    }
}
