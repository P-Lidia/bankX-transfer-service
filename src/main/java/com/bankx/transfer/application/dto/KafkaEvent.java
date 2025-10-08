package com.bankx.transfer.application.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Универсальный класс для представления событий Kafka.
 * Соответствует форматам сообщений из технического задания.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaEvent {

    /**
     * Уникальный идентификатор события
     */
    private String eventId;

    /**
     * Тип события (TRANSFER_COMMAND, DEBIT_REQUEST, etc.)
     */
    private String eventType;

    /**
     * Временная метка события
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private Instant timestamp;

    /**
     * Correlation ID для трассировки распределенных транзакций
     */
    private String correlationId;

    /**
     * ID перевода (опционально, для событий связанных с переводами)
     */
    private String transferId;

    /**
     * Полезная нагрузка события
     */
    private Map<String, Object> payload;

    /**
     * Создает событие с текущей временной меткой
     */
    public static KafkaEvent create(String eventType, String correlationId, Map<String, Object> payload) {
        return KafkaEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(eventType)
                .timestamp(Instant.now())
                .correlationId(correlationId)
                .payload(payload)
                .build();
    }

    /**
     * Создает событие с transferId
     */
    public static KafkaEvent createWithTransferId(String eventType, String correlationId,
                                                  String transferId, Map<String, Object> payload) {
        return KafkaEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(eventType)
                .timestamp(Instant.now())
                .correlationId(correlationId)
                .transferId(transferId)
                .payload(payload)
                .build();
    }
}