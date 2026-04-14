package com.bigsmo.service.service;

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MetricsFormer {

    public List<LogMetricEvent> formMetrics(List<NormalizedLogEvent> events,
                                            Instant windowStart,
                                            Instant windowEnd) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }

        Map<String, Counter> countersByService = new LinkedHashMap<>();

        for (NormalizedLogEvent event : events) {
            String serviceId = normalizeServiceId(event.getServiceId());
            Counter counter = countersByService.computeIfAbsent(serviceId, k -> new Counter());

            counter.total++;

            String level = normalizeLevel(event.getLevel());
            switch (level) {
                case "INFO" -> counter.info++;
                case "WARN" -> counter.warn++;
                case "ERROR" -> counter.error++;
                case "DEBUG" -> counter.debug++;
                default -> counter.other++;
            }
        }

        Instant createdAt = Instant.now();
        List<LogMetricEvent> result = new ArrayList<>();

        for (Map.Entry<String, Counter> entry : countersByService.entrySet()) {
            String serviceId = entry.getKey();
            Counter c = entry.getValue();

            result.add(buildMetric("logs_total", serviceId, c.total, windowStart, windowEnd, createdAt));
            result.add(buildMetric("logs_info_total", serviceId, c.info, windowStart, windowEnd, createdAt));
            result.add(buildMetric("logs_warn_total", serviceId, c.warn, windowStart, windowEnd, createdAt));
            result.add(buildMetric("logs_error_total", serviceId, c.error, windowStart, windowEnd, createdAt));
            result.add(buildMetric("logs_debug_total", serviceId, c.debug, windowStart, windowEnd, createdAt));

            if (c.other > 0) {
                result.add(buildMetric("logs_other_total", serviceId, c.other, windowStart, windowEnd, createdAt));
            }
        }

        return result;
    }

    private LogMetricEvent buildMetric(String metricName,
                                       String serviceId,
                                       long value,
                                       Instant windowStart,
                                       Instant windowEnd,
                                       Instant createdAt) {
        return LogMetricEvent.builder()
                .metricName(metricName)
                .serviceId(serviceId)
                .value(value)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .createdAt(createdAt)
                .build();
    }

    private String normalizeServiceId(String serviceId) {
        if (serviceId == null || serviceId.isBlank()) {
            return "unknown-service";
        }
        return serviceId.trim();
    }

    private String normalizeLevel(String level) {
        if (level == null || level.isBlank()) {
            return "INFO";
        }
        return level.trim().toUpperCase();
    }

    private static class Counter {
        long total;
        long info;
        long warn;
        long error;
        long debug;
        long other;
    }
}