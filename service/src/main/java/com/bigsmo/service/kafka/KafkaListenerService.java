package com.bigsmo.service.kafka;

import com.bigsmo.common.dto.IncomingLogDto;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.service.LogMetricsBatcher;
import com.bigsmo.service.service.LogNormalizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaListenerService {

    private final LogNormalizer logNormalizer;
    private final KafkaPublisherService publisher;
    private final ObjectMapper objectMapper;
    private final LogMetricsBatcher metricsBatcher;

    @KafkaListener(
            topics = "${app.kafka.topics.raw-logs}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(String rawMessage) {
        try {
            log.info("Received raw log: {}", rawMessage);

            IncomingLogDto incoming = objectMapper.readValue(rawMessage, IncomingLogDto.class);
            incoming.validate();

            NormalizedLogEvent normalized = logNormalizer.normalize(incoming, "kafka");

            // 1. Publish the normalized log IMMEDIATELY
            publisher.publishNormalizedLog(normalized);

            // 2. Queue it for later batched metrics
            metricsBatcher.addToBatch(normalized);

            log.info("Processed and queued for metrics: service={}, level={}",
                    normalized.getServiceId(),
                    normalized.getLevel());

        } catch (Exception e) {
            log.error("Failed to process raw log: {}", rawMessage, e);
        }
//        try {
//            log.info(" Received raw log: {}", rawMessage);
//
//            IncomingLogDto incoming = objectMapper.readValue(rawMessage, IncomingLogDto.class);
//            incoming.validate();
//
//            NormalizedLogEvent normalized = logNormalizer.normalize(incoming, "kafka");
//
//            Instant now = Instant.now();
//            List<LogMetricEvent> metrics = metricsFormer.formMetrics(
//                    Collections.singletonList(normalized),
//                    now,
//                    now
//            );
//
//            publisher.publishNormalizedLog(normalized);
//            publisher.publishMetrics(metrics);
//
//            log.info(" Processed: service={}, level={}, metrics_count={}",
//                    normalized.getServiceId(),
//                    normalized.getLevel(),
//                    metrics.size());
//
//        } catch (Exception e) {
//            log.error(" Failed to process raw log: {}", rawMessage, e);
//        }
    }
}