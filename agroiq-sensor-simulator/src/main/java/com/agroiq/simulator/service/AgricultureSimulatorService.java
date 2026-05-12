package com.agroiq.simulator.service;

import com.agroiq.simulator.config.KafkaTopicConfig;
import com.agroiq.simulator.model.FarmSimulationCommand;
import com.agroiq.simulator.model.SensorEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@RequiredArgsConstructor
public class AgricultureSimulatorService {

    private final KafkaTemplate<String, SensorEvent> kafkaTemplate;

    @Value("${app.simulator.agriculture.initial-farm-ids:FARM-101,FARM-102,FARM-103,FARM-104,FARM-105}")
    private String initialFarmIds;

    @Value("${app.simulator.agriculture.sensors-per-farm:3}")
    private int sensorsPerFarm;

    @Value("${app.simulator.agriculture.anomaly-probability:0.05}")
    private double anomalyProbability;

    @Value("${app.simulator.agriculture.anomaly-duration-ms:1800000}")
    private long anomalyDurationMs;

    private final Map<String, FarmState> farmStates = new ConcurrentHashMap<>();
    private final AtomicLong generatedEvents = new AtomicLong();
    private final AtomicLong publishedEvents = new AtomicLong();
    private final AtomicLong failedPublishes = new AtomicLong();
    private final AtomicLong invalidEvents = new AtomicLong();
    private final AtomicLong commandsHandled = new AtomicLong();

    public enum AnomalyProfile {
        NONE,
        DROUGHT_STRESS,
        FUNGAL_DISEASE_RISK,
        FLOOD_RISK,
        PH_IMBALANCE,
        HIGH_TEMP_STRESS
    }

    private static class FarmState {
        String farmId;
        List<String> sensorIds;
        float baseSoilMoisture;
        float basePh;

        AnomalyProfile currentAnomaly = AnomalyProfile.NONE;
        long anomalyEndTime = 0;

        FarmState(String farmId, int sensorsPerFarm) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            this.farmId = farmId;
            this.sensorIds = buildSensorIds(farmId, sensorsPerFarm);
            this.baseSoilMoisture = 55.0f + random.nextFloat() * 10.0f;
            this.basePh = 6.2f + random.nextFloat() * 0.6f;
        }

        private static List<String> buildSensorIds(String farmId, int sensorsPerFarm) {
            int sensorCount = Math.max(1, sensorsPerFarm);
            List<String> ids = new ArrayList<>(sensorCount);
            for (int i = 1; i <= sensorCount; i++) {
                ids.add(String.format("SENS-%s-%02d", farmId, i));
            }
            return ids;
        }
    }

    @PostConstruct
    public void init() {
        parseInitialFarmIds().forEach(this::registerFarm);
        log.info("Initialized simulator with {} farms, {} sensors per farm, anomalyDurationMs={}",
                farmStates.size(), Math.max(1, sensorsPerFarm), anomalyDurationMs);
    }

    @Scheduled(fixedRateString = "${app.simulator.agriculture.tick-rate-ms:5000}")
    public void simulateTick() {
        Instant now = Instant.now();
        long currentEpoch = now.toEpochMilli();
        int currentHour = LocalDateTime.now(ZoneId.systemDefault()).getHour();

        for (FarmState state : farmStates.values()) {
            List<SensorEvent> events = new ArrayList<>(state.sensorIds.size());
            synchronized (state) {
                updateFarmState(state, currentEpoch);
                for (String sensorId : state.sensorIds) {
                    events.add(generateEvent(state, sensorId, now, currentHour));
                }
            }

            events.forEach(this::publishSensorEvent);
        }
    }

    @Scheduled(fixedRateString = "${app.simulator.agriculture.stats-log-rate-ms:60000}")
    public void logSimulatorStats() {
        log.info("Simulator stats: farms={}, generatedEvents={}, publishedEvents={}, failedPublishes={}, invalidEvents={}, commandsHandled={}",
                farmStates.size(),
                generatedEvents.get(),
                publishedEvents.get(),
                failedPublishes.get(),
                invalidEvents.get(),
                commandsHandled.get());
    }

    @KafkaListener(
            topics = KafkaTopicConfig.SIMULATOR_COMMANDS_TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void handleFarmSimulationCommand(FarmSimulationCommand command) {
        if (command == null || command.getCommandType() == null || !StringUtils.hasText(command.getFarmId())) {
            log.warn("Ignoring invalid farm simulation command: {}", command);
            return;
        }

        commandsHandled.incrementAndGet();
        String farmId = command.getFarmId().trim();

        switch (command.getCommandType()) {
            case REGISTER_FARM:
                registerFarm(farmId);
                break;
            case TRIGGER_ANOMALY:
                triggerAnomaly(farmId, command.getAnomalyType());
                break;
            case CLEAR_ANOMALY:
                clearAnomaly(farmId);
                break;
            default:
                log.warn("Ignoring unsupported farm simulation command: {}", command);
                break;
        }
    }

    private List<String> parseInitialFarmIds() {
        return Arrays.stream(initialFarmIds.split(","))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .distinct()
                .toList();
    }

    private void registerFarm(String farmId) {
        FarmState newState = new FarmState(farmId, sensorsPerFarm);
        FarmState existingState = farmStates.putIfAbsent(farmId, newState);

        if (existingState == null) {
            log.info("Registered farm {} for simulation with {} sensors", farmId, newState.sensorIds.size());
        } else {
            log.info("Farm {} is already registered for simulation", farmId);
        }
    }

    private void triggerAnomaly(String farmId, String anomalyType) {
        FarmState state = farmStates.get(farmId);
        if (state == null) {
            log.warn("Cannot trigger anomaly {} for unknown farm {}", anomalyType, farmId);
            return;
        }

        AnomalyProfile anomalyProfile = parseAnomalyProfile(anomalyType);
        if (anomalyProfile == null || anomalyProfile == AnomalyProfile.NONE) {
            log.warn("Cannot trigger unsupported anomaly {} for farm {}", anomalyType, farmId);
            return;
        }

        long anomalyEndTime = Instant.now().toEpochMilli() + anomalyDurationMs;
        synchronized (state) {
            state.currentAnomaly = anomalyProfile;
            state.anomalyEndTime = anomalyEndTime;
            applyAnomalyBaselineOverride(state, anomalyProfile);
        }

        log.warn("Triggered command anomaly {} for farm {} until {}",
                anomalyProfile, farmId, Instant.ofEpochMilli(anomalyEndTime));
    }

    private void clearAnomaly(String farmId) {
        FarmState state = farmStates.get(farmId);
        if (state == null) {
            log.warn("Cannot clear anomaly for unknown farm {}", farmId);
            return;
        }

        AnomalyProfile clearedAnomaly;
        synchronized (state) {
            clearedAnomaly = state.currentAnomaly;
            state.currentAnomaly = AnomalyProfile.NONE;
            state.anomalyEndTime = 0;
            resetRecoveryBaselines(state);
        }

        log.info("Cleared anomaly {} for farm {}", clearedAnomaly, farmId);
    }

    private AnomalyProfile parseAnomalyProfile(String anomalyType) {
        if (!StringUtils.hasText(anomalyType)) {
            return null;
        }

        try {
            return AnomalyProfile.valueOf(anomalyType.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private void applyAnomalyBaselineOverride(FarmState state, AnomalyProfile anomalyProfile) {
        switch (anomalyProfile) {
            case DROUGHT_STRESS:
                state.baseSoilMoisture = Math.min(state.baseSoilMoisture, 12.0f);
                break;
            case FLOOD_RISK:
                state.baseSoilMoisture = Math.max(state.baseSoilMoisture, 96.0f);
                break;
            case PH_IMBALANCE:
                state.basePh = ThreadLocalRandom.current().nextBoolean() ? 4.8f : 8.2f;
                break;
            case FUNGAL_DISEASE_RISK:
            case HIGH_TEMP_STRESS:
            case NONE:
            default:
                break;
        }
    }

    private void updateFarmState(FarmState state, long currentEpoch) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        if (state.currentAnomaly != AnomalyProfile.NONE && currentEpoch > state.anomalyEndTime) {
            log.info("Anomaly {} ended for farm {}", state.currentAnomaly, state.farmId);
            state.currentAnomaly = AnomalyProfile.NONE;
            resetRecoveryBaselines(state);
        } else if (state.currentAnomaly == AnomalyProfile.NONE && random.nextDouble() < anomalyProbability) {
            AnomalyProfile[] profiles = AnomalyProfile.values();
            state.currentAnomaly = profiles[1 + random.nextInt(profiles.length - 1)];
            state.anomalyEndTime = currentEpoch + anomalyDurationMs;
            applyAnomalyBaselineOverride(state, state.currentAnomaly);
            log.warn("Randomly injecting anomaly {} for farm {} until {}",
                    state.currentAnomaly, state.farmId, Instant.ofEpochMilli(state.anomalyEndTime));
        }

        if (state.currentAnomaly != AnomalyProfile.FLOOD_RISK) {
            state.baseSoilMoisture -= 0.1f + random.nextFloat() * 0.2f;
        }

        state.baseSoilMoisture = clamp(state.baseSoilMoisture, 0.0f, 100.0f);
    }

    private void resetRecoveryBaselines(FarmState state) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (state.baseSoilMoisture < 20 || state.baseSoilMoisture > 90) {
            state.baseSoilMoisture = 50.0f + random.nextFloat() * 20.0f;
        }
        if (state.basePh < 5.5 || state.basePh > 7.5) {
            state.basePh = 6.2f + random.nextFloat() * 0.6f;
        }
    }

    private SensorEvent generateEvent(FarmState state, String sensorId, Instant timestamp, int currentHour) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        double timeOffset = ((currentHour - 4.0) / 24.0) * 2 * Math.PI;
        double diurnalFactor = Math.sin(timeOffset - Math.PI / 2);

        float baseTemp = 20.0f + (float) (diurnalFactor * 10.0f) + (random.nextFloat() * 2.0f - 1.0f);
        float baseHumidity = 60.0f - (float) (diurnalFactor * 20.0f) + (random.nextFloat() * 5.0f - 2.5f);

        float pH = state.basePh + (random.nextFloat() * 0.1f - 0.05f);
        float moisture = state.baseSoilMoisture;
        float temp = baseTemp;
        float humidity = baseHumidity;

        switch (state.currentAnomaly) {
            case DROUGHT_STRESS:
                moisture = Math.min(moisture, 14.9f - random.nextFloat() * 5.0f);
                temp = Math.max(temp, 32.1f + random.nextFloat() * 5.0f);
                break;
            case FUNGAL_DISEASE_RISK:
                humidity = Math.max(humidity, 85.1f + random.nextFloat() * 10.0f);
                temp = 22.0f + random.nextFloat() * 6.0f;
                break;
            case FLOOD_RISK:
                moisture = Math.max(moisture, 95.1f + random.nextFloat() * 4.9f);
                state.baseSoilMoisture = moisture;
                break;
            case PH_IMBALANCE:
                pH = random.nextBoolean()
                        ? 4.9f - random.nextFloat() * 1.0f
                        : 8.1f + random.nextFloat() * 1.0f;
                break;
            case HIGH_TEMP_STRESS:
                temp = Math.max(temp, 39.1f + random.nextFloat() * 5.0f);
                break;
            case NONE:
            default:
                break;
        }

        moisture = clamp(moisture, 0, 100);
        humidity = clamp(humidity, 0, 100);
        pH = clamp(pH, 0, 14);

        generatedEvents.incrementAndGet();
        return SensorEvent.builder()
                .farmId(state.farmId)
                .sensorId(sensorId)
                .soilMoisture(moisture)
                .temperature(temp)
                .humidity(humidity)
                .pH(pH)
                .timestamp(timestamp.toString())
                .build();
    }

    private void publishSensorEvent(SensorEvent event) {
        if (!isValidSensorEvent(event)) {
            invalidEvents.incrementAndGet();
            log.warn("Skipping invalid sensor event: {}", event);
            return;
        }

        kafkaTemplate.send(KafkaTopicConfig.RAW_SENSORS_TOPIC, event.getFarmId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        failedPublishes.incrementAndGet();
                        log.error("Failed to publish sensor event for farm {} sensor {}",
                                event.getFarmId(), event.getSensorId(), ex);
                    } else {
                        publishedEvents.incrementAndGet();
                        log.debug("Published sensor event to topic={}, partition={}, offset={}, farmId={}, sensorId={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                event.getFarmId(),
                                event.getSensorId());
                    }
                });
    }

    private boolean isValidSensorEvent(SensorEvent event) {
        return event != null
                && StringUtils.hasText(event.getFarmId())
                && StringUtils.hasText(event.getSensorId())
                && StringUtils.hasText(event.getTimestamp())
                && isWithin(event.getSoilMoisture(), 0, 100)
                && isWithin(event.getHumidity(), 0, 100)
                && isWithin(event.getPH(), 0, 14);
    }

    private boolean isWithin(float value, float min, float max) {
        return !Float.isNaN(value) && value >= min && value <= max;
    }

    private float clamp(float value, float min, float max) {
        return Math.max(min, Math.min(max, value));
    }
}
