package com.agroiq.simulator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorEvent {

    /**
     * Farm Identifier (Must be the Kafka Partition Key)
     */
    private String farmId;

    /**
     * Unique identifier for the sensor
     */
    private String sensorId;

    /**
     * Soil Moisture Percentage (0-100)
     */
    private float soilMoisture;

    /**
     * Temperature in Celsius
     */
    private float temperature;

    /**
     * Humidity Percentage (0-100)
     */
    private float humidity;

    /**
     * Soil pH (0-14)
     */
    private float pH;

    /**
     * ISO 8601 UTC timestamp
     */
    private String timestamp;
}
