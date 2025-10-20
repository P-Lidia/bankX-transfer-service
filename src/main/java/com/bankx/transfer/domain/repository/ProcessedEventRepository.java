package com.bankx.transfer.domain.repository;

/**
 * Порт для работы с обработанными событиями (идемпотентность)
 */
public interface ProcessedEventRepository {
    boolean existsByEventId(String eventId);
    void save(String eventId);
    void deleteOldProcessedEvents(java.time.LocalDateTime beforeDate);
}