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

        assertEquals(5, findValue(metrics, "auth", "logs_total"));
        assertEquals(2, findValue(metrics, "auth", "logs_info_total"));
        assertEquals(1, findValue(metrics, "auth", "logs_warn_total"));
        assertEquals(1, findValue(metrics, "auth", "logs_error_total"));
        assertEquals(1, findValue(metrics, "auth", "logs_debug_total"));
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

        assertEquals(2, findValue(metrics, "auth", "logs_total"));
        assertEquals(1, findValue(metrics, "auth", "logs_info_total"));
        assertEquals(1, findValue(metrics, "auth", "logs_warn_total"));
        assertEquals(0, findValue(metrics, "auth", "logs_error_total"));

        assertEquals(2, findValue(metrics, "billing", "logs_total"));
        assertEquals(2, findValue(metrics, "billing", "logs_error_total"));
        assertEquals(0, findValue(metrics, "billing", "logs_warn_total"));
    }

    private long findValue(List<LogMetricEvent> metrics, String serviceId, String metricName) {
        return metrics.stream()
                .filter(m -> serviceId.equals(m.getServiceId()))
                .filter(m -> metricName.equals(m.getMetricName()))
                .map(LogMetricEvent::getValue)
                .findFirst()
                .orElse(0L);
    }
}