package com.agroiq.simulator.model;

import lombok.Data;

@Data
public class FarmSimulationCommand {

    private CommandType commandType;
    private String farmId;
    private String anomalyType;

    public enum CommandType {
        REGISTER_FARM,
        TRIGGER_ANOMALY,
        CLEAR_ANOMALY
    }
}
