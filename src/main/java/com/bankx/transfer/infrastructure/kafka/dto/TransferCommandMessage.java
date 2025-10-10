package com.bankx.transfer.infrastructure.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * TransferCommandMessage — DTO-обьект, описывающий структуру входящего Kafka-сообщения,
 * которое отправляется из Orchestrator Service в Transfer Service.
 * Используется для передачи команды на выполнение перевода в рамках саги.
 * Структура :
 * - метаданные события
 * - payload — данные самого перевода
 */

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransferCommandMessage {

    private String eventId;
    private String eventType;
    private Instant timestamp;
    private String correlationId;
    private Payload payload;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Payload {
        private String fromAccount;
        private String toAccount;
        private BigDecimal amount;
        private String currency;
        private String description;
    }
}

