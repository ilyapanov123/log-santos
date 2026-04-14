package com.bigsmo.service.service;

import com.bigsmo.common.dto.IncomingLogDto;
import com.bigsmo.common.model.NormalizedLogEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

@Service
public class LogNormalizer {

    public NormalizedLogEvent normalize(IncomingLogDto dto, String clientIp) {
        return NormalizedLogEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .serviceId(dto.getServiceId())
                .timestamp(parseTimestamp(dto.getTimestamp()))
                .level(normalizeLevel(dto.getLevel()))
                .message(dto.getMessage())
                .traceId(dto.getTraceId())
                .spanId(dto.getSpanId())
                .attrs(dto.getAttrs())
                .ingestedAt(Instant.now())
                .sourceIp(clientIp)
                .normalizedAt(Instant.now())
                .build();
    }

    private Instant parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return Instant.now();
        }

        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException e) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(raw));
            } catch (NumberFormatException ex) {
                return Instant.now();
            }
        }
    }

    private String normalizeLevel(String level) {
        if (level == null || level.isBlank()) {
            return "INFO";
        }
        return level.toUpperCase();
    }
}