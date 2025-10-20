package com.bankx.transfer.application.port;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Порт для публикации событий в Kafka
 * Соответствует требованиям ТЗ по интеграции через Kafka
 */
public interface EventPublisherPort {

    // Основные события Saga согласно ТЗ
    CompletableFuture<Void> publishDebitRequest(String correlationId, String transferId, Map<String, Object> payload);
    CompletableFuture<Void> publishCreditRequest(String correlationId, String transferId, Map<String, Object> payload);
    CompletableFuture<Void> publishCompensateDebit(String correlationId, String transferId, Map<String, Object> payload);
    CompletableFuture<Void> publishTransferStatus(String eventType, String correlationId, String transferId, Map<String, Object> payload);

    // Универсальный метод
    CompletableFuture<Void> publishEvent(String topic, String key, Map<String, Object> event);
}