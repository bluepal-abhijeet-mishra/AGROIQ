package com.agroiq.simulator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "app.weather")
public class WeatherIngestionProperties {

    private OpenWeatherMap openweathermap = new OpenWeatherMap();
    private Ingestion ingestion = new Ingestion();
    private Map<String, Region> regions = new LinkedHashMap<>();

    @Data
    public static class OpenWeatherMap {
        private String baseUrl = "https://api.openweathermap.org/data/2.5/weather";
        private String apiKey = "demo_key";
        private long requestTimeoutMs = 10000;
    }

    @Data
    public static class Ingestion {
        private long pollRateMs = 900000;
    }

    @Data
    public static class Region {
        private double lat;
        private double lon;
    }
}
