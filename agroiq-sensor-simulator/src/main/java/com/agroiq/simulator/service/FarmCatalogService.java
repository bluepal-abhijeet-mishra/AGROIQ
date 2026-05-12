package com.agroiq.simulator.service;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@Service
public class FarmCatalogService {

    private final Set<String> farmIds = new ConcurrentSkipListSet<>();

    public boolean registerFarm(String farmId) {
        if (!StringUtils.hasText(farmId)) {
            return false;
        }
        return farmIds.add(farmId.trim());
    }

    public List<String> getFarmIds() {
        return List.copyOf(farmIds);
    }
}
