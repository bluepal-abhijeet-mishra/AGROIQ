package com.agroiq.simulator.service;

import com.agroiq.simulator.config.KafkaTopicConfig;
import com.agroiq.simulator.config.WeatherIngestionProperties;
import com.agroiq.simulator.model.WeatherEvent;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class WeatherApiIngestionService {

    private static final float METERS_PER_SECOND_TO_KM_PER_HOUR = 3.6f;

    private final WebClient webClient;
    private final KafkaTemplate<String, WeatherEvent> kafkaTemplate;
    private final WeatherIngestionProperties properties;

    @Scheduled(fixedRateString = "${app.weather.ingestion.poll-rate-ms:900000}")
    public void ingestWeatherForConfiguredRegions() {
        if (properties.getRegions().isEmpty()) {
            log.warn("Weather ingestion skipped because no regions are configured");
            return;
        }

        properties.getRegions().forEach(this::fetchAndPublishWeather);
    }

    private void fetchAndPublishWeather(String regionId, WeatherIngestionProperties.Region region) {
        webClient.get()
                .uri(properties.getOpenweathermap().getBaseUrl()
                                + "?lat={lat}&lon={lon}&appid={apiKey}&units=metric",
                        region.getLat(),
                        region.getLon(),
                        properties.getOpenweathermap().getApiKey())
                .retrieve()
                .onStatus(this::isRecoverableWeatherApiError, response ->
                        response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new WeatherApiException(
                                        response.statusCode(),
                                        "OpenWeatherMap request failed for region " + regionId + ": " + body))))
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofMillis(properties.getOpenweathermap().getRequestTimeoutMs()))
                .map(response -> mapToWeatherEvent(regionId, response))
                .doOnNext(this::publishWeatherEvent)
                .onErrorResume(ex -> {
                    logWeatherIngestionFailure(regionId, ex);
                    return Mono.empty();
                })
                .subscribe();
    }

    private boolean isRecoverableWeatherApiError(HttpStatusCode statusCode) {
        return statusCode.value() == 429 || statusCode.is5xxServerError();
    }

    private WeatherEvent mapToWeatherEvent(String regionId, JsonNode response) {
        JsonNode main = response.path("main");
        JsonNode wind = response.path("wind");
        JsonNode rain = response.path("rain");

        WeatherEvent event = new WeatherEvent();
        event.setRegionId(regionId);
        event.setTemperature((float) main.path("temp").asDouble());
        event.setHumidity((float) main.path("humidity").asDouble());
        event.setRainfallMm((float) rain.path("1h").asDouble(0.0d));
        event.setWindSpeed((float) wind.path("speed").asDouble(0.0d) * METERS_PER_SECOND_TO_KM_PER_HOUR);
        event.setTimestamp(resolveTimestamp(response));
        return event;
    }

    private long resolveTimestamp(JsonNode response) {
        long apiTimestampSeconds = response.path("dt").asLong(0);
        if (apiTimestampSeconds > 0) {
            return apiTimestampSeconds * 1000;
        }
        return Instant.now().toEpochMilli();
    }

    private void publishWeatherEvent(WeatherEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.WEATHER_DATA_TOPIC, event.getRegionId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish weather event for region {}", event.getRegionId(), ex);
                        return;
                    }

                    log.info("Published weather event for region {} to topic={}, partition={}, offset={}",
                            event.getRegionId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                });
    }

    private void logWeatherIngestionFailure(String regionId, Throwable ex) {
        Throwable root = unwrap(ex);
        if (root instanceof WeatherApiException apiException) {
            log.warn("Weather API call failed for region {} with status {}: {}",
                    regionId, apiException.statusCode.value(), apiException.getMessage());
        } else if (root instanceof TimeoutException) {
            log.warn("Weather API call timed out for region {}", regionId);
        } else {
            log.warn("Weather API ingestion failed for region {}: {}", regionId, root.getMessage(), root);
        }
    }

    private Throwable unwrap(Throwable ex) {
        Throwable current = ex;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static class WeatherApiException extends RuntimeException {
        private final HttpStatusCode statusCode;

        private WeatherApiException(HttpStatusCode statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }
    }
}
