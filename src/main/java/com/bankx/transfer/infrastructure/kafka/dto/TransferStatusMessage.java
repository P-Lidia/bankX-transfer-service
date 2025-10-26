package com.bankx.transfer.infrastructure.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * TransferStatusMessage — DTO-объект, описывающий структуру исходящего Kafka-сообщения в топик transfer.status.
 * - Используется Transfer Service  при отправке финального статуса перевода.
 * - Отражает результат выполнения всей саги.
 * Структура:
 * - Метаданные события (eventId, eventType, timestamp, correlationId, transferId)
 * - Payload — подробности результата перевода
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransferStatusMessage {
    private String eventId;
    private String eventType;
    private Instant timestamp;
    private UUID correlationId;
    private UUID transferId;
    private StatusPayload payload;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StatusPayload {
        private String fromAccount;
        private String toAccount;
        private BigDecimal amount;
        private String currency;
        private String status;
        private Instant completedAt;
        private String debitTransactionId;
        private String creditTransactionId;
    }
}

