# AgroIQ Simulator Integration Guide

## 1. Purpose

The AgroIQ Sensor Simulator is the development-time data provider for the AgroIQ platform. It produces realistic ingestion-layer data so backend, stream-processing, risk-engine, recommendation, alerting, and dashboard teams can build and test their modules without waiting for real farm hardware, weather pipelines, or satellite integrations.

The simulator currently provides:

- IoT farm sensor events
- Live weather events from OpenWeatherMap
- Simulated satellite / NDVI crop health events
- Runtime farm registration commands
- Runtime anomaly trigger and clear commands
- PRD-aligned Kafka topic provisioning
- Health and Prometheus-compatible observability endpoints

This service is an ingestion simulator. It does not implement the downstream risk engine, recommendation engine, database persistence, alert dispatcher, analytics APIs, or frontend dashboard.

## 2. Service Location

Module:

```text
agroiq-sensor-simulator
```

Main Spring Boot class:

```text
com.agroiq.simulator.SimulatorApplication
```

Default application name:

```text
agroiq-sensor-simulator
```

## 3. Prerequisites

Before starting the simulator, ensure the following are available:

- Java 17
- Maven
- Apache Kafka reachable by the simulator
- OpenWeatherMap API key for live weather ingestion

Default Kafka broker:

```text
localhost:9092
```

If another team runs Kafka on a different host, update `spring.kafka.producer.bootstrap-servers` and `spring.kafka.consumer.bootstrap-servers` in `application.yml`.

## 4. Running The Simulator

From the simulator module:

```bash
cd agroiq-sensor-simulator
mvn spring-boot:run
```

Recommended environment variable for weather ingestion:

```bash
WEATHER_API_KEY=<your_openweathermap_api_key>
```

If no API key is provided, the simulator uses the configured fallback value:

```text
demo_key
```

The fallback key is only for local bootstrapping. Live weather calls require a valid OpenWeatherMap API key.

## 5. Kafka Topics

The simulator provisions the PRD-aligned Kafka topics below.

| Topic | Producer | Partitions | Retention | Kafka Key | Purpose |
|---|---:|---:|---:|---|---|
| `farm.sensors.raw` | Yes | 12 | 3 days | `farmId` | Raw IoT farm sensor readings |
| `weather.data` | Yes | 6 | 7 days | `regionId` | Weather readings by region |
| `satellite.data` | Yes | 4 | 30 days | `farmId` | Simulated satellite / NDVI crop health data |
| `farm.sensors.enriched` | No | 12 | 7 days | `farmId` | Expected output from stream enrichment team |
| `crop.risk.alerts` | No | 6 | 90 days | `farmId` | Expected output from risk engine team |
| `farm.recommendations` | No | 6 | 30 days | `farmId` | Expected output from recommendation engine |
| `analytics.metrics` | No | 4 | 90 days | `metricType` | Expected analytics / ML metrics topic |
| `farm-simulator-commands` | Consumer | 3 | 7 days | `farmId` | Runtime simulator control commands |

Downstream services should consume the producer topics:

```text
farm.sensors.raw
weather.data
satellite.data
```

## 6. Kafka Consumer Configuration For Teammates

Example Spring Boot consumer configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: agroiq-risk-engine
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

Each downstream service should use its own `group-id`.

Example group IDs:

```text
agroiq-stream-enrichment
agroiq-risk-engine
agroiq-recommendation-engine
agroiq-alert-dispatcher
agroiq-dashboard-api
```

## 7. Sensor Event Data

Topic:

```text
farm.sensors.raw
```

Kafka key:

```text
farmId
```

Schema:

```json
{
  "farmId": "FARM-101",
  "sensorId": "SENS-FARM-101-01",
  "soilMoisture": 61.4,
  "temperature": 28.2,
  "humidity": 47.9,
  "pH": 6.5,
  "timestamp": "2026-05-12T06:30:00.000Z"
}
```

Field notes:

| Field | Type | Description |
|---|---|---|
| `farmId` | string | Farm identifier and Kafka partition key |
| `sensorId` | string | Sensor identifier within the farm |
| `soilMoisture` | float | Percentage, `0-100` |
| `temperature` | float | Celsius |
| `humidity` | float | Percentage, `0-100` |
| `pH` | float | Soil pH, `0-14` |
| `timestamp` | string | ISO 8601 UTC timestamp |

Default behavior:

- Emits events on a scheduled tick.
- Uses sinusoidal daily temperature and humidity patterns.
- Supports multiple sensors per farm.
- Can inject random or command-triggered anomalies.
- Uses `farmId` as the Kafka key to preserve per-farm ordering.

## 8. Weather Event Data

Topic:

```text
weather.data
```

Kafka key:

```text
regionId
```

Schema:

```json
{
  "regionId": "REG-NORTH",
  "temperature": 23.7,
  "rainfallMm": 0.0,
  "humidity": 68.0,
  "windSpeed": 12.4,
  "timestamp": 1778567400000
}
```

Field notes:

| Field | Type | Description |
|---|---|---|
| `regionId` | string | Region identifier and Kafka partition key |
| `temperature` | float | Celsius |
| `rainfallMm` | float | Millimeters. Defaults to `0.0` when OpenWeatherMap has no `rain.1h` field |
| `humidity` | float | Percentage, `0-100` |
| `windSpeed` | float | Kilometers per hour |
| `timestamp` | long | Epoch milliseconds |

Default behavior:

- Runs every 15 minutes.
- Calls OpenWeatherMap Current Weather endpoint.
- Uses `units=metric`.
- Converts OpenWeatherMap wind speed from meters/second to kilometers/hour.
- Logs API failures and continues without crashing the application.
- Handles timeout, HTTP `429`, and HTTP `5xx` failures gracefully.

Configured regions are in `application.yml`:

```yaml
app:
  weather:
    regions:
      REG-NORTH:
        lat: 47.6
        lon: -122.3
      REG-CENTRAL:
        lat: 39.7
        lon: -104.9
      REG-SOUTH:
        lat: 29.7
        lon: -95.3
```

## 9. Satellite / NDVI Event Data

Topic:

```text
satellite.data
```

Kafka key:

```text
farmId
```

Schema:

```json
{
  "farmId": "FARM-101",
  "regionId": "REG-NORTH",
  "ndvi": 0.742,
  "cloudCoverPct": 18.3,
  "cropStress": false,
  "source": "SIMULATED_NDVI",
  "capturedAt": "2026-05-12T06:30:00.000Z"
}
```

Field notes:

| Field | Type | Description |
|---|---|---|
| `farmId` | string | Farm identifier and Kafka partition key |
| `regionId` | string | Region identifier |
| `ndvi` | float | Simulated NDVI value |
| `cloudCoverPct` | float | Simulated cloud cover percentage |
| `cropStress` | boolean | `true` when NDVI is below configured stress threshold |
| `source` | string | Currently `SIMULATED_NDVI` |
| `capturedAt` | string | ISO 8601 UTC timestamp |

Default behavior:

- Runs on a scheduled satellite scan interval.
- Produces one satellite event per registered farm.
- Uses dynamically registered farms from the simulator farm catalog.
- Simulates normal and crop-stress NDVI values.

## 10. Runtime Simulator Commands

The simulator listens to:

```text
farm-simulator-commands
```

Kafka key:

```text
farmId
```

Command schema:

```json
{
  "commandType": "REGISTER_FARM",
  "farmId": "FARM-999",
  "anomalyType": null
}
```

Supported command types:

| Command | Description |
|---|---|
| `REGISTER_FARM` | Adds a new farm to the simulator without restart |
| `TRIGGER_ANOMALY` | Forces a farm into a specific anomaly profile |
| `CLEAR_ANOMALY` | Clears the active anomaly for a farm |

Supported anomaly types:

```text
DROUGHT_STRESS
FUNGAL_DISEASE_RISK
FLOOD_RISK
PH_IMBALANCE
HIGH_TEMP_STRESS
```

Register farm example:

```json
{
  "commandType": "REGISTER_FARM",
  "farmId": "FARM-999",
  "anomalyType": null
}
```

Trigger drought anomaly example:

```json
{
  "commandType": "TRIGGER_ANOMALY",
  "farmId": "FARM-999",
  "anomalyType": "DROUGHT_STRESS"
}
```

Clear anomaly example:

```json
{
  "commandType": "CLEAR_ANOMALY",
  "farmId": "FARM-999",
  "anomalyType": null
}
```

## 11. How Downstream Teams Should Connect

### Stream Processing Team

Consume:

```text
farm.sensors.raw
weather.data
satellite.data
```

Produce:

```text
farm.sensors.enriched
crop.risk.alerts
analytics.metrics
```

Expected use:

- Join sensor data with latest weather by region.
- Compute rolling sensor statistics.
- Use satellite NDVI data for crop stress indicators.

### Risk Engine Team

Consume:

```text
farm.sensors.enriched
satellite.data
```

Produce:

```text
crop.risk.alerts
```

Expected use:

- Detect drought stress.
- Detect fungal disease risk.
- Detect flood risk.
- Detect soil pH imbalance.
- Detect high temperature stress.
- Optionally use `satellite.data.cropStress` as an additional risk signal.

### Recommendation Team

Consume:

```text
crop.risk.alerts
```

Produce:

```text
farm.recommendations
```

Expected use:

- Convert risk alerts into irrigation, pesticide, fertilizer, drainage, or heat-stress recommendations.

### Dashboard / API Team

Consume directly only if needed:

```text
farm.sensors.raw
weather.data
satellite.data
crop.risk.alerts
farm.recommendations
```

Recommended approach:

- Prefer consuming from backend APIs or persisted stores once those modules exist.
- During early development, direct Kafka consumption is acceptable for live panels and integration tests.

## 12. Configuration Reference

Important simulator settings:

```yaml
app:
  simulator:
    agriculture:
      tick-rate-ms: 5000
      stats-log-rate-ms: 60000
      initial-farm-ids: FARM-101,FARM-102,FARM-103,FARM-104,FARM-105
      sensors-per-farm: 3
      anomaly-probability: 0.05
      anomaly-duration-ms: 1800000
    satellite:
      scan-rate-ms: 21600000
      stats-log-rate-ms: 300000
      default-region-id: REG-NORTH
      crop-stress-probability: 0.08
      ndvi-stress-threshold: 0.35
  weather:
    openweathermap:
      base-url: https://api.openweathermap.org/data/2.5/weather
      api-key: ${WEATHER_API_KEY:demo_key}
      request-timeout-ms: 10000
    ingestion:
      poll-rate-ms: 900000
```

Tuning guidance:

- Increase `sensors-per-farm` for higher sensor throughput.
- Lower `tick-rate-ms` for faster event generation.
- Increase `anomaly-probability` for more frequent random risk scenarios.
- Use `farm-simulator-commands` for deterministic anomaly tests.
- Lower `satellite.scan-rate-ms` in local testing if teams need faster NDVI events.
- Keep weather polling at 15 minutes unless the OpenWeatherMap account supports higher usage.

## 13. Observability

The simulator exposes Spring Actuator endpoints.

Available endpoints:

```text
/actuator/health
/actuator/info
/actuator/metrics
/actuator/prometheus
```

These endpoints are useful for:

- Service health checks
- Local debugging
- Prometheus scraping
- Basic operational monitoring

The simulator also logs periodic stats for:

- registered farms
- generated sensor events
- published sensor events
- failed Kafka publishes
- invalid sensor events
- handled simulator commands
- satellite event publishing statistics

## 14. Reliability Behavior

Sensor and satellite publishing:

- Events are validated before publish.
- Kafka publish callbacks log success/failure.
- Failed publishes are counted in logs.

Weather ingestion:

- API calls are asynchronous via Spring WebClient.
- Timeout is configurable.
- HTTP `429` and `5xx` responses are logged.
- Missing `rain.1h` safely becomes `0.0`.
- Failure for one region does not stop ingestion for other regions.

Thread safety:

- Farm state is stored in concurrent collections.
- Per-farm mutable simulation state is synchronized during update/generation.
- Dynamic farm registration can happen while scheduled simulation is running.

## 15. Important Boundaries

The simulator is responsible for providing realistic ingestion streams.

The simulator is not responsible for:

- Kafka Streams enrichment implementation
- Crop risk detection engine
- Smart recommendation engine
- PostgreSQL / TimescaleDB persistence
- Redis caching
- Alert dispatching through SMS / push / WebSocket
- REST dashboard APIs
- React dashboard UI
- Authentication and RBAC
- ML prediction services

Those modules should consume simulator data and implement their own PRD responsibilities.

## 16. Quick Integration Checklist

Before integrating, each teammate should confirm:

- Kafka is running and reachable.
- Simulator is running.
- Their service uses the same Kafka bootstrap server.
- Their consumer group ID is unique.
- They consume the correct topic.
- Their DTO matches the JSON shape documented above.
- They handle replay or duplicate events idempotently where needed.
- They do not assume every farm has weather or satellite data at the exact same timestamp.

## 17. Summary

The AgroIQ simulator is ready to act as the project ingestion data provider. It gives teams realistic Kafka streams for farm sensors, weather, and satellite/NDVI data, while also supporting dynamic farms and deterministic anomaly testing. Teams should build their modules by consuming the documented Kafka topics and treating this simulator as the source of development and integration test data.
