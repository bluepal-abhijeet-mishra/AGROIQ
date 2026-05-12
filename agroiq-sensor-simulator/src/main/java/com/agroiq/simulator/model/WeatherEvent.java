package com.agroiq.simulator.model;

import lombok.Data;

@Data
public class WeatherEvent {

    /**
     * Region identifier. Must be the Kafka partition key.
     */
    private String regionId;

    /**
     * Temperature in Celsius.
     */
    private float temperature;

    /**
     * Rainfall in millimeters.
     */
    private float rainfallMm;

    /**
     * Relative humidity percentage.
     */
    private float humidity;

    /**
     * Wind speed in km/h.
     */
    private float windSpeed;

    /**
     * Epoch milliseconds.
     */
    private long timestamp;
}
