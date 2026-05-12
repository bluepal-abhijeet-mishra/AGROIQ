package com.agroiq.simulator.service;

import com.agroiq.simulator.config.KafkaTopicConfig;
import com.agroiq.simulator.model.SatelliteEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@RequiredArgsConstructor
public class SatelliteDataSimulatorService {

    private static final String SOURCE = "SIMULATED_NDVI";

    private final KafkaTemplate<String, SatelliteEvent> kafkaTemplate;
    private final FarmCatalogService farmCatalogService;

    @Value("${app.simulator.satellite.default-region-id:REG-NORTH}")
    private String defaultRegionId;

    @Value("${app.simulator.satellite.crop-stress-probability:0.08}")
    private double cropStressProbability;

    @Value("${app.simulator.satellite.ndvi-stress-threshold:0.35}")
    private float ndviStressThreshold;

    private final AtomicLong generatedEvents = new AtomicLong();
    private final AtomicLong publishedEvents = new AtomicLong();
    private final AtomicLong failedPublishes = new AtomicLong();

    @Scheduled(fixedRateString = "${app.simulator.satellite.scan-rate-ms:21600000}")
    public void simulateSatelliteScan() {
        List<String> farmIds = farmCatalogService.getFarmIds();
        if (farmIds.isEmpty()) {
            log.warn("Satellite simulation skipped because no farms are registered");
            return;
        }

        Instant capturedAt = Instant.now();
        farmIds.stream()
                .map(farmId -> generateSatelliteEvent(farmId, capturedAt))
                .filter(this::isValidSatelliteEvent)
                .forEach(this::publishSatelliteEvent);
    }

    @Scheduled(fixedRateString = "${app.simulator.satellite.stats-log-rate-ms:300000}")
    public void logSatelliteStats() {
        log.info("Satellite simulator stats: farms={}, generatedEvents={}, publishedEvents={}, failedPublishes={}",
                farmCatalogService.getFarmIds().size(),
                generatedEvents.get(),
                publishedEvents.get(),
                failedPublishes.get());
    }

    private SatelliteEvent generateSatelliteEvent(String farmId, Instant capturedAt) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean stress = random.nextDouble() < cropStressProbability;
        float ndvi = stress
                ? 0.15f + random.nextFloat() * 0.20f
                : 0.55f + random.nextFloat() * 0.30f;

        SatelliteEvent event = new SatelliteEvent();
        event.setFarmId(farmId);
        event.setRegionId(defaultRegionId);
        event.setNdvi(round(ndvi));
        event.setCloudCoverPct(round(random.nextFloat() * 45.0f));
        event.setCropStress(event.getNdvi() < ndviStressThreshold);
        event.setSource(SOURCE);
        event.setCapturedAt(capturedAt.toString());

        generatedEvents.incrementAndGet();
        return event;
    }

    private boolean isValidSatelliteEvent(SatelliteEvent event) {
        boolean valid = event != null
                && StringUtils.hasText(event.getFarmId())
                && StringUtils.hasText(event.getRegionId())
                && StringUtils.hasText(event.getCapturedAt())
                && event.getNdvi() >= -1.0f
                && event.getNdvi() <= 1.0f
                && event.getCloudCoverPct() >= 0.0f
                && event.getCloudCoverPct() <= 100.0f;

        if (!valid) {
            log.warn("Skipping invalid satellite event: {}", event);
        }
        return valid;
    }

    private void publishSatelliteEvent(SatelliteEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.SATELLITE_DATA_TOPIC, event.getFarmId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        failedPublishes.incrementAndGet();
                        log.error("Failed to publish satellite event for farm {}", event.getFarmId(), ex);
                    } else {
                        publishedEvents.incrementAndGet();
                        log.debug("Published satellite event to topic={}, partition={}, offset={}, farmId={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                event.getFarmId());
                    }
                });
    }

    private float round(float value) {
        return Math.round(value * 1000.0f) / 1000.0f;
    }
}
