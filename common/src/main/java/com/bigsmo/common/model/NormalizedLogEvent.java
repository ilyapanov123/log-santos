package com.bigsmo.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NormalizedLogEvent {
    private String eventId;
    private String serviceId;
    private Instant timestamp;
    private String level;
    private String message;
    private String traceId;
    private String spanId;
    private Map<String, Object> attrs;
    private Instant ingestedAt;
    private String sourceIp;
    private Instant normalizedAt;

    public Map<String, Object> toMap() {
        return Map.of(
                "event_id", eventId,
                "service_id", serviceId,
                "@timestamp", timestamp,
                "level", level,
                "message", message,
                "trace_id", traceId != null ? traceId : "",
                "span_id", spanId != null ? spanId : "",
                "attrs", attrs != null ? attrs : Map.of(),
                "ingested_at", ingestedAt
        );
    }
}