package com.bigsmo.service.service;

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MetricsFormer {

    public List<LogMetricEvent> formMetrics(NormalizedLogEvent event) {
        List<LogMetricEvent> metrics = new ArrayList<>();

        metrics.add(LogMetricEvent.builder()
                .metricName("logs_total")
                .serviceId(event.getServiceId())
                .level(event.getLevel())
                .value(1L)
                .timestamp(event.getTimestamp())
                .build());

        if ("WARN".equalsIgnoreCase(event.getLevel())) {
            metrics.add(LogMetricEvent.builder()
                    .metricName("logs_warn_total")
                    .serviceId(event.getServiceId())
                    .level(event.getLevel())
                    .value(1L)
                    .timestamp(event.getTimestamp())
                    .build());
        }

        if ("ERROR".equalsIgnoreCase(event.getLevel())) {
            metrics.add(LogMetricEvent.builder()
                    .metricName("logs_error_total")
                    .serviceId(event.getServiceId())
                    .level(event.getLevel())
                    .value(1L)
                    .timestamp(event.getTimestamp())
                    .build());
        }

        return metrics;
    }
}