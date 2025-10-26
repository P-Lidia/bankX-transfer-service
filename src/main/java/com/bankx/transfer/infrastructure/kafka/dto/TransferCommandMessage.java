package com.bankx.transfer.infrastructure.kafka.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * TransferCommandMessage — DTO-обьект, описывающий структуру входящего Kafka-сообщения,
 * которое отправляется из Payment Orchestrator в Transfer Service.
 * Используется для передачи команды на выполнение перевода в рамках саги.
 * Структура :
 * - метаданные события
 * - payload — данные самого перевода
 */

@Data
public class TransferCommandMessage {
    private String eventId;
    private String eventType;
    private Instant timestamp;
    private UUID correlationId;
    private Payload payload;

    @Data
    public static class Payload {
        private String fromAccount;
        private String toAccount;
        private BigDecimal amount;
        private String currency;
        private String description;
    }

    // Геттеры для удобства доступа к полям payload
    public String getFromAccount() {
        return payload != null ? payload.getFromAccount() : null;
    }

    public String getToAccount() {
        return payload != null ? payload.getToAccount() : null;
    }

    public BigDecimal getAmount() {
        return payload != null ? payload.getAmount() : null;
    }

    public String getCurrency() {
        return payload != null ? payload.getCurrency() : null;
    }

    public String getDescription() {
        return payload != null ? payload.getDescription() : null;
    }
}