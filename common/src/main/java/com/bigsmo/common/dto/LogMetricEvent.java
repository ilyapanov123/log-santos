package com.bigsmo.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogMetricEvent {
    private String metricName;
    private String serviceId;
    private String level;
    private long value;
    private Instant timestamp;
}