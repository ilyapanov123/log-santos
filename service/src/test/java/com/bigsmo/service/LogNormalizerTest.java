package com.bigsmo.service;

import com.bigsmo.common.dto.IncomingLogDto;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.service.LogNormalizer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LogNormalizerTest {

    private final LogNormalizer logNormalizer = new LogNormalizer();

    @Test
    void shouldNormalizeIncomingLog() {
        IncomingLogDto dto = new IncomingLogDto();
        dto.setServiceId("auth");
        dto.setTimestamp("2026-04-07T14:30:00Z");
        dto.setLevel("info");
        dto.setMessage("User authentication started");
        dto.setTraceId("trace-001");
        dto.setSpanId("span-001");
        dto.setAttrs(Map.of("user_id", "123"));

        NormalizedLogEvent event = logNormalizer.normalize(dto, "127.0.0.1");

        assertNotNull(event.getEventId());
        assertEquals("auth", event.getServiceId());
        assertEquals("INFO", event.getLevel());
        assertEquals("User authentication started", event.getMessage());
        assertEquals("trace-001", event.getTraceId());
        assertEquals("span-001", event.getSpanId());
        assertEquals("127.0.0.1", event.getSourceIp());
        assertNotNull(event.getTimestamp());
        assertNotNull(event.getIngestedAt());
        assertNotNull(event.getNormalizedAt());
        assertEquals("123", event.getAttrs().get("user_id"));
    }

    @Test
    void shouldUseDefaultLevelIfMissing() {
        IncomingLogDto dto = new IncomingLogDto();
        dto.setServiceId("auth");
        dto.setMessage("Test message");

        NormalizedLogEvent event = logNormalizer.normalize(dto, "127.0.0.1");

        assertEquals("INFO", event.getLevel());
    }

    @Test
    void shouldUseCurrentTimeIfTimestampInvalid() {
        IncomingLogDto dto = new IncomingLogDto();
        dto.setServiceId("auth");
        dto.setTimestamp("invalid-timestamp");
        dto.setMessage("Test message");

        NormalizedLogEvent event = logNormalizer.normalize(dto, "127.0.0.1");

        assertNotNull(event.getTimestamp());
    }
}