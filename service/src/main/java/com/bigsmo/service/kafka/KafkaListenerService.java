package com.bigsmo.service.kafka;

import com.bigsmo.common.dto.IncomingLogDto;
import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.service.LogNormalizer;
import com.bigsmo.service.service.MetricsFormer;
import com.bigsmo.service.config.KafkaTopicsProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaListenerService {

    private final LogNormalizer logNormalizer;
    private final MetricsFormer metricsFormer;
    private final KafkaPublisherService publisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.raw-logs}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(String rawMessage) {
        try {
            log.info(" Received raw log: {}", rawMessage);

            IncomingLogDto incoming = objectMapper.readValue(rawMessage, IncomingLogDto.class);
            incoming.validate();

            NormalizedLogEvent normalized = logNormalizer.normalize(incoming, "kafka");

            Instant now = Instant.now();
            List<LogMetricEvent> metrics = metricsFormer.formMetrics(
                    Collections.singletonList(normalized),
                    now,
                    now
            );

            publisher.publishNormalizedLog(normalized);
            publisher.publishMetrics(metrics);

            log.info(" Processed: service={}, level={}, metrics_count={}", 
                    normalized.getServiceId(), 
                    normalized.getLevel(), 
                    metrics.size());

        } catch (Exception e) {
            log.error(" Failed to process raw log: {}", rawMessage, e);
        }
    }
}