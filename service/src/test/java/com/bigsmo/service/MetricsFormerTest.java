package com.bigsmo.service;

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.service.MetricsFormer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetricsFormerTest {

    private final MetricsFormer metricsFormer = new MetricsFormer();

    @Test
    void shouldCreateOnlyTotalMetricForInfo() {
        NormalizedLogEvent event = NormalizedLogEvent.builder()
                .serviceId("auth")
                .level("INFO")
                .timestamp(Instant.now())
                .message("Info log")
                .build();

        List<LogMetricEvent> metrics = metricsFormer.formMetrics(event);

        assertEquals(1, metrics.size());
        assertEquals("logs_total", metrics.get(0).getMetricName());
    }

    @Test
    void shouldCreateWarnMetrics() {
        NormalizedLogEvent event = NormalizedLogEvent.builder()
                .serviceId("billing")
                .level("WARN")
                .timestamp(Instant.now())
                .message("Warn log")
                .build();

        List<LogMetricEvent> metrics = metricsFormer.formMetrics(event);

        assertEquals(2, metrics.size());
        assertEquals("logs_total", metrics.get(0).getMetricName());
        assertEquals("logs_warn_total", metrics.get(1).getMetricName());
    }

    @Test
    void shouldCreateErrorMetrics() {
        NormalizedLogEvent event = NormalizedLogEvent.builder()
                .serviceId("billing")
                .level("ERROR")
                .timestamp(Instant.now())
                .message("Error log")
                .build();

        List<LogMetricEvent> metrics = metricsFormer.formMetrics(event);

        assertEquals(2, metrics.size());
        assertEquals("logs_total", metrics.get(0).getMetricName());
        assertEquals("logs_error_total", metrics.get(1).getMetricName());
    }
}