package com.agroiq.simulator.service;

import com.agroiq.simulator.config.KafkaTopicConfig;
import com.agroiq.simulator.model.SensorEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class AgricultureSimulatorService {

    private final KafkaTemplate<String, SensorEvent> kafkaTemplate;

    @Value("${app.simulator.agriculture.anomaly-probability:0.05}")
    private double anomalyProbability;

    @Value("${app.simulator.agriculture.anomaly-duration-ms:30000}")
    private long anomalyDurationMs;

    private final Random random = new Random();

    // Internal state tracking for farms
    private final Map<String, FarmState> farmStates = new ConcurrentHashMap<>();

    private final List<String> farmIds = Arrays.asList(
            "FARM-101", "FARM-102", "FARM-103", "FARM-104", "FARM-105"
    );

    // Enums for Anomaly Profiles
    public enum AnomalyProfile {
        NONE,
        DROUGHT_STRESS,
        FUNGAL_DISEASE_RISK,
        FLOOD_RISK,
        PH_IMBALANCE,
        HIGH_TEMP_STRESS
    }

    // Internal state class for a Farm
    private static class FarmState {
        String farmId;
        String sensorId;
        float baseSoilMoisture;
        float basePh;

        // Anomaly state
        AnomalyProfile currentAnomaly = AnomalyProfile.NONE;
        long anomalyEndTime = 0;

        FarmState(String farmId) {
            this.farmId = farmId;
            this.sensorId = "SENS-" + farmId + "-01";
            this.baseSoilMoisture = 60.0f; // Start at 60%
            this.basePh = 6.5f; // Baseline pH
        }
    }

    // Initialize farm states
    @jakarta.annotation.PostConstruct
    public void init() {
        farmIds.forEach(id -> farmStates.put(id, new FarmState(id)));
        log.info("Initialized {} farm states for simulation", farmStates.size());
    }

    @Scheduled(fixedRateString = "${app.simulator.agriculture.tick-rate-ms:5000}")
    public void simulateTick() {
        long currentEpoch = Instant.now().toEpochMilli();
        int currentHour = LocalDateTime.now(ZoneId.systemDefault()).getHour();

        for (FarmState state : farmStates.values()) {
            updateFarmState(state, currentEpoch, currentHour);
            SensorEvent event = generateEvent(state, currentEpoch, currentHour);

            // Publish to Kafka using farmId as partition key
            kafkaTemplate.send(KafkaTopicConfig.RAW_SENSORS_TOPIC, event.getFarmId(), event);
            log.debug("Published event for farm {}: {}", event.getFarmId(), event);
        }
    }

    private void updateFarmState(FarmState state, long currentEpoch, int currentHour) {
        // Handle Anomaly transitions
        if (state.currentAnomaly != AnomalyProfile.NONE && currentEpoch > state.anomalyEndTime) {
            log.info("Anomaly {} ended for farm {}", state.currentAnomaly, state.farmId);
            state.currentAnomaly = AnomalyProfile.NONE;
            // Slightly reset moisture if recovering from extreme anomaly
            if (state.baseSoilMoisture < 20 || state.baseSoilMoisture > 90) {
                 state.baseSoilMoisture = 50.0f + (random.nextFloat() * 20.0f); // Reset between 50-70%
            }
        } else if (state.currentAnomaly == AnomalyProfile.NONE && random.nextDouble() < anomalyProbability) {
            AnomalyProfile[] profiles = AnomalyProfile.values();
            // Pick a random anomaly excluding NONE
            state.currentAnomaly = profiles[1 + random.nextInt(profiles.length - 1)];
            state.anomalyEndTime = currentEpoch + anomalyDurationMs;
            log.warn("INJECTING ANOMALY: {} for farm {} until {}", state.currentAnomaly, state.farmId, Instant.ofEpochMilli(state.anomalyEndTime));
        }

        // Apply normal decay (only if not currently forced by an anomaly that overrides decay behavior)
        // Let's always apply a small decay to the base to simulate drying out over time
        if (state.currentAnomaly != AnomalyProfile.FLOOD_RISK) {
            state.baseSoilMoisture -= 0.1f + (random.nextFloat() * 0.2f); // Slow decay
        }

        // Ensure bounds
        state.baseSoilMoisture = Math.max(0.0f, Math.min(100.0f, state.baseSoilMoisture));
    }

    private SensorEvent generateEvent(FarmState state, long currentEpoch, int currentHour) {
        // Base diurnal cycle logic (0 to 23 hours)
        // Peak temp around 14:00 (2 PM), Lowest around 04:00 (4 AM)
        // Sinusoidal normalized from -1 to 1 based on hour
        double timeOffset = ((currentHour - 4.0) / 24.0) * 2 * Math.PI;
        double diurnalFactor = Math.sin(timeOffset - Math.PI/2); // -1 at 4AM, 1 at 4PM

        // Normal base values
        float baseTemp = 20.0f + (float)(diurnalFactor * 10.0f) + (random.nextFloat() * 2.0f - 1.0f); // 10 to 30 C
        float baseHumidity = 60.0f - (float)(diurnalFactor * 20.0f) + (random.nextFloat() * 5.0f - 2.5f); // 40 to 80 % (lower when hot)

        // pH is relatively stable with micro-fluctuations
        float pH = state.basePh + (random.nextFloat() * 0.1f - 0.05f);
        float moisture = state.baseSoilMoisture;
        float temp = baseTemp;
        float humidity = baseHumidity;

        // Override if in Anomaly
        switch (state.currentAnomaly) {
            case DROUGHT_STRESS:
                moisture = Math.min(moisture, 14.9f - random.nextFloat() * 5.0f); // Force < 15%
                temp = Math.max(temp, 32.1f + random.nextFloat() * 5.0f); // Force > 32C
                break;
            case FUNGAL_DISEASE_RISK:
                humidity = Math.max(humidity, 85.1f + random.nextFloat() * 10.0f); // Force > 85%
                temp = 22.0f + random.nextFloat() * 6.0f; // Force between 22-28C
                break;
            case FLOOD_RISK:
                moisture = Math.max(moisture, 95.1f + random.nextFloat() * 4.9f); // Force > 95%
                state.baseSoilMoisture = moisture; // Update base so it stays wet for a bit
                break;
            case PH_IMBALANCE:
                if (random.nextBoolean()) {
                    pH = 4.9f - random.nextFloat() * 1.0f; // Force < 5.0
                } else {
                    pH = 8.1f + random.nextFloat() * 1.0f; // Force > 8.0
                }
                break;
            case HIGH_TEMP_STRESS:
                temp = Math.max(temp, 39.1f + random.nextFloat() * 5.0f); // Force > 39C
                break;
            case NONE:
            default:
                break;
        }

        // Clamp values
        moisture = clamp(moisture, 0, 100);
        humidity = clamp(humidity, 0, 100);
        pH = clamp(pH, 0, 14);

        return SensorEvent.builder()
                .farmId(state.farmId)
                .sensorId(state.sensorId)
                .soilMoisture(moisture)
                .temperature(temp)
                .humidity(humidity)
                .pH(pH)
                .timestamp(currentEpoch)
                .build();
    }

    private float clamp(float value, float min, float max) {
        return Math.max(min, Math.min(max, value));
    }
}
