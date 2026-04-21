package com.bigsmo.service.kafka; 

import com.bigsmo.common.dto.LogMetricEvent;
import com.bigsmo.common.kaffka.KafkaPublisher;
import com.bigsmo.common.model.NormalizedLogEvent;
import com.bigsmo.service.config.KafkaTopicsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaPublisherService implements KafkaPublisher<Object> {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;

    @Override
    public void publish(Object message) {
        if (message instanceof NormalizedLogEvent) {
            publishNormalizedLog((NormalizedLogEvent) message);
        } else if (message instanceof List) {
            List<?> list = (List<?>) message;
            if (!list.isEmpty() && list.get(0) instanceof LogMetricEvent) {
                publishMetrics((List<LogMetricEvent>) list);
            }
        }
    }

    public void publishNormalizedLog(NormalizedLogEvent event) {
        log.debug("Publishing normalized log to {}: serviceId={}", 
                topics.getNormalizedLogs(), event.getServiceId());
        kafkaTemplate.send(topics.getNormalizedLogs(), event.getServiceId(), event);
    }

    public void publishMetrics(List<LogMetricEvent> metrics) {
        for (LogMetricEvent metric : metrics) {
            log.debug("Publishing metric to {}: name={}", 
                    topics.getMetrics(), metric.getMetricName());
            kafkaTemplate.send(topics.getMetrics(), metric);
        }
    }
}