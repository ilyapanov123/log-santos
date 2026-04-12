package com.bigsmo.controller.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.bigsmo.common.kaffka.KafkaPublisher;

@Service
@RequiredArgsConstructor
public class RawLogKafkaPublisher implements KafkaPublisher<String> {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publish(String data) {
        kafkaTemplate.send("raw-log", data);
    }
}
