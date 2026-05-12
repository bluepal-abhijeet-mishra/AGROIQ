package com.agroiq.simulator.model;

import lombok.Data;

@Data
public class SatelliteEvent {

    /**
     * Farm identifier. Must be the Kafka partition key.
     */
    private String farmId;

    private String regionId;
    private float ndvi;
    private float cloudCoverPct;
    private boolean cropStress;
    private String source;
    private String capturedAt;
}
