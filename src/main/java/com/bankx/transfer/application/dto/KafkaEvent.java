package com.bankx.transfer.application.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Универсальное DTO для событий Kafka.
 * Используется как для входящих, так и для исходящих событий.
 * Гарантирует единообразие структуры сообщений между микросервисами.
 * Соответствует требованиям ТЗ по формату сообщений (5.1).
 *
 * Структура события:
 * - eventId: уникальный идентификатор события
 * - eventType: тип бизнес-события
 * - timestamp: время создания события
 * - correlationId: идентификатор корреляции для трассировки
 * - transferId: идентификатор перевода (опционально)
 * - payload: полезная нагрузка в формате JSON
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaEvent {

    /**
     * Уникальный идентификатор события.
     * Генерируется отправителем для каждой публикации.
     * Используется для идемпотентной обработки на стороне получателя.
     */
    private String eventId;

    /**
     * Тип бизнес-события (DEBIT_REQUEST, CREDIT_CONFIRMED и т.д.).
     * Определяет семантику события и обработчик на стороне получателя.
     * Соответствует событиям из технического задания (2.2).
     */
    private String eventType;

    /**
     * Временная метка создания события.
     * Используется для мониторинга задержек и аудита.
     * Формат соответствует ISO 8601.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private Instant timestamp;

    /**
     * Идентификатор корреляции для трассировки распределенных транзакций.
     * Остается неизменным на протяжении всей Saga.
     * Соответствует требованию ТЗ по идемпотентности (2.1).
     */
    private UUID correlationId;

    /**
     * Идентификатор перевода, к которому относится событие.
     * Опциональное поле, присутствует только в событиях, связанных с переводами.
     * Используется для связи событий с конкретным бизнес-объектом.
     */
    private UUID transferId;

    /**
     * Полезная нагрузка события в формате JSON.
     * Содержит бизнес-данные, специфичные для типа события.
     * Структура payload зависит от eventType.
     */
    private Object payload;

    /**
     * Создает событие с привязкой к конкретному переводу.
     * Используется для событий, которые являются частью Saga процесса перевода.
     * Автоматически генерирует eventId и устанавливает текущее время.
     *
     * @param eventType тип события (DEBIT_REQUEST, CREDIT_CONFIRMED и т.д.)
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода (строковый)
     * @param payload полезная нагрузка события
     * @return сконфигурированный объект KafkaEvent
     *
     * @implNote Метод используется для создания событий, связанных с конкретным переводом.
     * Пример: создание DEBIT_REQUEST при инициации перевода.
     */
    public static KafkaEvent createWithTransferId(String eventType, UUID correlationId,
                                                  String transferId, Object payload) {
        KafkaEvent event = new KafkaEvent();
        event.setEventId("evt_" + UUID.randomUUID());
        event.setEventType(eventType);
        event.setCorrelationId(correlationId);
        event.setTransferId(UUID.fromString(transferId));
        event.setPayload(payload);
        event.setTimestamp(Instant.now());
        return event;
    }

    /**
     * Создает общее событие без привязки к конкретному переводу.
     * Используется для системных или административных событий.
     * Автоматически генерирует eventId и устанавливает текущее время.
     *
     * @param eventType тип события
     * @param correlationId идентификатор корреляции для трассировки
     * @param payload полезная нагрузка события
     * @return сконфигурированный объект KafkaEvent
     *
     * @implNote Метод используется для событий, не связанных с конкретным переводом.
     * Пример: системные уведомления, метрики, аудиторские события.
     */
    public static KafkaEvent create(String eventType, UUID correlationId, Object payload) {
        KafkaEvent event = new KafkaEvent();
        event.setEventId("evt_" + UUID.randomUUID());
        event.setEventType(eventType);
        event.setCorrelationId(correlationId);
        event.setPayload(payload);
        event.setTimestamp(Instant.now());
        return event;
    }

    /**
     * Проверяет, привязано ли событие к конкретному переводу.
     * Используется для определения типа обработки события.
     *
     * @return true если событие имеет transferId, false в противном случае
     */
    public boolean hasTransferId() {
        return transferId != null;
    }

    /**
     * Проверяет, является ли событие частью Saga процесса перевода.
     * Определяется по наличию correlationId и transferId.
     *
     * @return true если событие является частью Saga, false в противном случае
     */
    public boolean isSagaEvent() {
        return correlationId != null && transferId != null;
    }

    /**
     * Получает строковое представление correlationId для логирования.
     * Возвращает null-safe строку.
     *
     * @return строковое представление correlationId или "null"
     */
    public String getCorrelationIdString() {
        return correlationId != null ? correlationId.toString() : "null";
    }

    /**
     * Получает строковое представление transferId для логирования.
     * Возвращает null-safe строку.
     *
     * @return строковое представление transferId или "null"
     */
    public String getTransferIdString() {
        return transferId != null ? transferId.toString() : "null";
    }

    /**
     * Builder класс для удобного создания KafkaEvent.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder для KafkaEvent.
     */
    public static class Builder {
        private String eventId;
        private String eventType;
        private Instant timestamp;
        private UUID correlationId;
        private UUID transferId;
        private Object payload;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder correlationId(UUID correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder transferId(UUID transferId) {
            this.transferId = transferId;
            return this;
        }

        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        public KafkaEvent build() {
            return new KafkaEvent(eventId, eventType, timestamp, correlationId, transferId, payload);
        }
    }
}