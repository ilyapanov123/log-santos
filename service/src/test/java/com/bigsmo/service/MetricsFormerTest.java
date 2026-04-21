package com.bigsmo.service;

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.service.MetricsFormer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsFormerTest {

    private final MetricsFormer metricsFormer = new MetricsFormer();

    @Test
    void shouldAggregateMetricsForSingleService() {
        List<NormalizedLogEvent> events = List.of(
                NormalizedLogEvent.builder().serviceId("auth").level("INFO").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("auth").level("INFO").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("auth").level("WARN").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("auth").level("ERROR").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("auth").level("DEBUG").timestamp(Instant.now()).build()
        );

        Instant start = Instant.now().minusSeconds(60);
        Instant end = Instant.now();

        List<LogMetricEvent> metrics = metricsFormer.formMetrics(events, start, end);

        assertEquals(5, findValue(metrics, "logs_total"));
        assertEquals(2, findValue(metrics, "logs_info_total"));
        assertEquals(1, findValue(metrics, "logs_warn_total"));
        assertEquals(1, findValue(metrics, "logs_error_total"));
        assertEquals(1, findValue(metrics, "logs_debug_total"));
    }

    @Test
    void shouldAggregateMetricsPerService() {
        List<NormalizedLogEvent> events = List.of(
                NormalizedLogEvent.builder().serviceId("auth").level("INFO").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("auth").level("WARN").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("billing").level("ERROR").timestamp(Instant.now()).build(),
                NormalizedLogEvent.builder().serviceId("billing").level("ERROR").timestamp(Instant.now()).build()
        );

        Instant start = Instant.now().minusSeconds(60);
        Instant end = Instant.now();

        List<LogMetricEvent> metrics = metricsFormer.formMetrics(events, start, end);

        assertEquals(2, findValue(metrics, "logs_total"));
        assertEquals(1, findValue(metrics, "logs_info_total"));
        assertEquals(1, findValue(metrics, "logs_warn_total"));
        assertEquals(0, findValue(metrics, "logs_error_total"));

        assertEquals(2, findValue(metrics, "logs_total"));
        assertEquals(2, findValue(metrics, "logs_error_total"));
        assertEquals(0, findValue(metrics, "logs_warn_total"));
    }

    private long findValue(List<LogMetricEvent> metrics, String metricName) {
        return metrics.stream()
                .filter(m -> metricName.equals(m.getMetricName()))
                .map(LogMetricEvent::getValue)
                .findFirst()
                .orElse(0L);
    }
}