# AgroIQ Sensor Simulator

## Overview

The AgroIQ Sensor Simulator is the ingestion data provider for local and integration development of the AgroIQ platform. It generates realistic Kafka event streams for farm sensors, weather data, and simulated satellite / NDVI observations so other teams can build stream processing, risk detection, recommendations, alerts, and dashboard features without depending on real field hardware.

The simulator is a Spring Boot application using Apache Kafka, scheduled producers, WebClient, and actuator-based observability.

## What The Simulator Provides

- Raw IoT sensor readings for farms
- Live weather data from OpenWeatherMap
- Simulated satellite / NDVI crop health readings
- Runtime farm registration through Kafka commands
- Runtime anomaly triggering for risk-engine testing
- PRD-aligned Kafka topic provisioning
- Health, metrics, and Prometheus actuator endpoints

The simulator does not implement the downstream risk engine, recommendation engine, database persistence, alert dispatching, REST APIs, or frontend dashboard.

## Prerequisites

Install and start the following before running the simulator:

- Java 17
- Maven
- Apache Kafka
- OpenWeatherMap API key for live weather ingestion

Default Kafka broker:

```text
localhost:9092
```

If Kafka is running on another host, update `src/main/resources/application.yml`.

## Configuration

Main configuration file:

```text
src/main/resources/application.yml
```

Important settings:

```yaml
spring:
  kafka:
    producer:
      bootstrap-servers: localhost:9092
    consumer:
      bootstrap-servers: localhost:9092

app:
  simulator:
    agriculture:
      tick-rate-ms: 5000
      initial-farm-ids: FARM-101,FARM-102,FARM-103,FARM-104,FARM-105
      sensors-per-farm: 3
      anomaly-probability: 0.05
      anomaly-duration-ms: 1800000
    satellite:
      scan-rate-ms: 21600000
      crop-stress-probability: 0.08
      ndvi-stress-threshold: 0.35
  weather:
    openweathermap:
      api-key: ${WEATHER_API_KEY:demo_key}
    ingestion:
      poll-rate-ms: 900000
```

Recommended environment variable:

```bash
WEATHER_API_KEY=<your_openweathermap_api_key>
```

On Windows PowerShell:

```powershell
$env:WEATHER_API_KEY="<your_openweathermap_api_key>"
```

## How To Start

From this module directory:

```bash
mvn spring-boot:run
```

From the repository root:

```bash
cd agroiq-sensor-simulator
mvn spring-boot:run
```

To verify the project builds:

```bash
mvn test
```

## How It Works

### Sensor Simulation

The simulator starts with configured farms such as:

```text
FARM-101, FARM-102, FARM-103, FARM-104, FARM-105
```

For each farm, it creates one or more sensors and emits readings on a scheduled tick. Sensor values follow a realistic daily cycle:

- temperature rises and falls by time of day
- humidity moves inversely to temperature
- soil moisture gradually changes over time
- pH stays mostly stable with small variations

Sensor events are published to:

```text
farm.sensors.raw
```

Kafka key:

```text
farmId
```

### Weather Ingestion

Every 15 minutes by default, the simulator calls the OpenWeatherMap Current Weather API for configured regions.

Weather events are published to:

```text
weather.data
```

Kafka key:

```text
regionId
```

If the weather API times out, returns rate limit errors, or returns server errors, the simulator logs the issue and continues running.

### Satellite / NDVI Simulation

The simulator generates synthetic satellite crop health events for each registered farm.

Satellite events are published to:

```text
satellite.data
```

Kafka key:

```text
farmId
```

The generated event includes:

- NDVI value
- cloud cover percentage
- crop stress flag
- capture timestamp

By default, a small percentage of farms receive low NDVI values to simulate crop stress.

## Kafka Topics

The simulator provisions these topics:

| Topic | Produced By Simulator | Partitions | Retention | Kafka Key |
|---|---:|---:|---:|---|
| `farm.sensors.raw` | Yes | 12 | 3 days | `farmId` |
| `weather.data` | Yes | 6 | 7 days | `regionId` |
| `satellite.data` | Yes | 4 | 30 days | `farmId` |
| `farm.sensors.enriched` | No | 12 | 7 days | `farmId` |
| `crop.risk.alerts` | No | 6 | 90 days | `farmId` |
| `farm.recommendations` | No | 6 | 30 days | `farmId` |
| `analytics.metrics` | No | 4 | 90 days | `metricType` |
| `farm-simulator-commands` | Consumed By Simulator | 3 | 7 days | `farmId` |

Downstream services should consume:

```text
farm.sensors.raw
weather.data
satellite.data
```

## Event Examples

### Sensor Event

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

### Weather Event

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

### Satellite Event

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

## Runtime Commands

The simulator listens for control messages on:

```text
farm-simulator-commands
```

Supported commands:

- `REGISTER_FARM`
- `TRIGGER_ANOMALY`
- `CLEAR_ANOMALY`

Supported anomaly types:

- `DROUGHT_STRESS`
- `FUNGAL_DISEASE_RISK`
- `FLOOD_RISK`
- `PH_IMBALANCE`
- `HIGH_TEMP_STRESS`

Example command to register a farm:

```json
{
  "commandType": "REGISTER_FARM",
  "farmId": "FARM-999",
  "anomalyType": null
}
```

Example command to trigger drought stress:

```json
{
  "commandType": "TRIGGER_ANOMALY",
  "farmId": "FARM-999",
  "anomalyType": "DROUGHT_STRESS"
}
```

Example command to clear an anomaly:

```json
{
  "commandType": "CLEAR_ANOMALY",
  "farmId": "FARM-999",
  "anomalyType": null
}
```

## Connecting Another Service

A teammate's Spring Boot service should use the same Kafka broker and consume the required topics.

Example consumer configuration:

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

Each downstream service should use a unique consumer group ID.

## Observability

The simulator exposes:

```text
/actuator/health
/actuator/info
/actuator/metrics
/actuator/prometheus
```

These endpoints can be used for local checks, Prometheus scraping, and deployment health probes.

The simulator also logs periodic statistics for generated events, successful publishes, failed publishes, invalid events, registered farms, and handled commands.

## Professional Usage Notes

- Keep Kafka running before starting the simulator.
- Use a real `WEATHER_API_KEY` for live weather data.
- Use `farm-simulator-commands` for deterministic testing scenarios.
- Do not make downstream services depend on exact event timing across sensor, weather, and satellite streams.
- Downstream services should be idempotent because Kafka consumers may reprocess events after restarts.
- For higher load testing, increase `sensors-per-farm` or lower `tick-rate-ms`.

## Detailed Team Guide

For complete integration details, see:

```text
SIMULATOR_INTEGRATION_GUIDE.md
```
