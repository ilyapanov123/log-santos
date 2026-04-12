package com.bigsmo.controller.service.impl;

import com.bigsmo.controller.service.LogIngestService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;


@Slf4j
@Service
@RequiredArgsConstructor
public class LogIngestServiceImpl implements LogIngestService {
    private final RawLogKafkaPublisher logKafkaPublisher;

    @Override
    public IngestResult ingest(List<String> rawLogs) {
        int accepted = 0;
        int dropped = 0;

        for (String logString : rawLogs) {
            try {
                logKafkaPublisher.publish(logString);
                accepted++;
            } catch (Exception e) {
                dropped++;
                log.debug("Uhhh something broke, the exception is {}", e.getMessage());
                log.debug("The string is \"{}\"", logString);
            }
        }
        return new IngestResult(accepted, dropped);
    }
}