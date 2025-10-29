package com.bankx.transfer.infrastructure.kafka.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * AccountEventMessage — DTO-объект, описывающий структуру входящего Kafka-сообщения из Account Service,
 * которое публикуется  после обработки дебета/кредита.
 * <p>
 * Используется для уведомления других микросервисов
 * о результате операции по счёту: успешном дебете, кредите или об ошибке.
 * <p>
 * Структура:
 * - метаданные (eventId, eventType, timestamp, correlationId)
 * - payload — данные конкретной операции по счёту
 */

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AccountEventMessage {
    private String eventId;
    private String eventType;
    private Instant timestamp;
    private UUID correlationId;
    private Payload payload;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Payload {
        private UUID transferId;
        private String account;
        private String debitTransactionId;
        private String creditTransactionId;
        private String reason;
    }
}


