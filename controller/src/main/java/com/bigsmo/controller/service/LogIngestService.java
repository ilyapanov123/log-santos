package com.bigsmo.controller.service;

import java.util.List;

public interface LogIngestService {
    public IngestResult ingest(List<String> events);

    public record IngestResult(int accepted, int dropped) {}
}