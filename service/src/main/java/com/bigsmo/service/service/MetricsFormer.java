package com.bigsmo.service.service;

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class MetricsFormer {

    // Inject the list from properties, defaulting to standard levels if not found
    @Value("${app.metrics.types:INFO,WARN,ERROR,DEBUG}")
    private List<String> configuredTypes;

    private Set<String> targetLevels;

    @PostConstruct
    public void init() {
        this.targetLevels = configuredTypes.stream()
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
    }

    public List<LogMetricEvent> formMetrics(List<NormalizedLogEvent> events,
                                            Instant windowStart,
                                            Instant windowEnd) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }

        long total = 0;
        long other = 0;

        Map<String, Long> levelCounts = new LinkedHashMap<>();

        // Pre-fill map with 0s so we always report metrics for configured levels,
        // even if no logs of that type occurred in this window.
        for (String level : targetLevels) {
            levelCounts.put(level, 0L);
        }

        for (NormalizedLogEvent event : events) {
            total++;

            String level = normalizeLevel(event.getLevel());
            if (targetLevels.contains(level)) {
                levelCounts.put(level, levelCounts.get(level) + 1);
            } else {
                other++;
            }
        }

        Instant createdAt = Instant.now();
        List<LogMetricEvent> result = new ArrayList<>();

        // Add total logs metric
        result.add(buildMetric("logs_total", total, windowStart, windowEnd, createdAt));

        // Add dynamically configured level metrics
        for (Map.Entry<String, Long> entry : levelCounts.entrySet()) {
            // Converts "CUSTOM_1" to "logs_custom_1"
            String metricName = "logs_" + entry.getKey().toLowerCase();
            result.add(buildMetric(metricName, entry.getValue(), windowStart, windowEnd, createdAt));
        }

        // Add "other" metric if we encountered unconfigured log levels
        if (other > 0) {
            result.add(buildMetric("logs_other", other, windowStart, windowEnd, createdAt));
        }

        return result;
    }

    private LogMetricEvent buildMetric(String metricName,
                                       long value,
                                       Instant windowStart,
                                       Instant windowEnd,
                                       Instant createdAt) {
        return LogMetricEvent.builder()
                .metricName(metricName)
                .value(value)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .createdAt(createdAt)
                .build();
    }

    private String normalizeLevel(String level) {
        if (level == null || level.isBlank()) {
            return "INFO"; // Defaulting to INFO when empty
        }
        return level.trim().toUpperCase();
    }
}