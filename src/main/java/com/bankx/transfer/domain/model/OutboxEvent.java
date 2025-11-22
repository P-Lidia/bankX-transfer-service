package com.bankx.transfer.domain.model;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Доменная модель для исходящих событий в паттерне Outbox.
 * Представляет событие, которое должно быть отправлено во внешнюю систему (Kafka).
 * Гарантирует надежную доставку событий даже при временной недоступности брокера сообщений.
 *
 * Основные характеристики:
 * - Сохраняется в БД в той же транзакции, что и бизнес-данные
 * - Отправляется в Kafka фоновым процессом (OutboxEventPublisher)
 * - Поддерживает retry логику при ошибках отправки
 * - Обеспечивает трассировку через correlationId
 *
 * Соответствует требованиям ТЗ по надежной доставке событий между сервисами.
 */
public class OutboxEvent {

    private final Long id;
    private final String aggregateType;
    private final UUID aggregateId;
    private final String eventType;
    private final String payload;
    private final UUID correlationId;
    private String status;
    private final LocalDateTime createdAt;
    private final LocalDateTime processedAt;
    private final Integer retryCount;
    private final String errorMessage;
    private final LocalDateTime updatedAt;

    /**
     * Конструктор для создания новых событий.
     * Гарантирует, что correlationId никогда не будет null.
     */
    public OutboxEvent(String aggregateType, UUID aggregateId, String eventType,
                       String payload, UUID correlationId) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId != null ? correlationId : UUID.randomUUID();

        this.status = "NEW";
        this.retryCount = 0;
        this.createdAt = LocalDateTime.now();
    }

    /**
     * Полный конструктор для восстановления из persistence слоя.
     */
    public OutboxEvent(Long id, String aggregateType, UUID aggregateId, String eventType,
                       String payload, UUID correlationId, String status, LocalDateTime createdAt,
                       LocalDateTime processedAt, Integer retryCount, String errorMessage,
                       LocalDateTime updatedAt) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.status = status;
        this.createdAt = createdAt;
        this.processedAt = processedAt;
        this.retryCount = retryCount;
        this.errorMessage = errorMessage;
        this.updatedAt = updatedAt;

        validate();
    }

    /**
     * Фабричный метод для создания нового события.
     * Используется бизнес-логикой при создании событий для отправки в Kafka.
     *
     * @param aggregateType тип агрегата-источника
     * @param aggregateId идентификатор агрегата
     * @param eventType тип бизнес-события
     * @param payload данные события в JSON
     * @param correlationId идентификатор корреляции
     * @return новый экземпляр OutboxEvent в статусе NEW
     */
    public static OutboxEvent create(String aggregateType, UUID aggregateId,
                                     String eventType, String payload, UUID correlationId) { // Изменено на UUID
        return new OutboxEvent(
                null, // ID будет сгенерирован БД
                aggregateType,
                aggregateId,
                eventType,
                payload,
                correlationId,
                "NEW",
                LocalDateTime.now(),
                null, // processedAt будет установлен при успешной отправке
                0, // начальное количество попыток
                null, // ошибки пока нет
                LocalDateTime.now()
        );
    }

    /**
     * Валидация инвариантов доменной модели.
     * Гарантирует корректность состояния объекта.
     *
     * @throws IllegalArgumentException если нарушены инварианты доменной модели
     */
    private void validate() {
        if (aggregateType == null || aggregateType.trim().isEmpty()) {
            throw new IllegalArgumentException("Aggregate type cannot be null or empty");
        }
        if (aggregateId == null) {
            throw new IllegalArgumentException("Aggregate ID cannot be null");
        }
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new IllegalArgumentException("Event type cannot be null or empty");
        }
        if (payload == null || payload.trim().isEmpty()) {
            throw new IllegalArgumentException("Payload cannot be null or empty");
        }
        if (correlationId == null) {
            throw new IllegalArgumentException("Correlation ID cannot be null");
        }
        if (status == null || !isValidStatus(status)) {
            throw new IllegalArgumentException("Invalid status: " + status);
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("Created at cannot be null");
        }
        if (retryCount == null || retryCount < 0) {
            throw new IllegalArgumentException("Retry count cannot be negative");
        }
    }

    private boolean isValidStatus(String status) {
        return "NEW".equals(status) || "PROCESSING".equals(status) ||
                "SENT".equals(status) || "FAILED".equals(status);
    }

    // ГЕТТЕРЫ:

    public Long getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    /**
     * Получить идентификатор корреляции для трассировки распределенных транзакций.
     * Соответствует требованию 2.1 ТЗ по идемпотентности и трассировке.
     */
    public UUID getCorrelationId() {
        return correlationId;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    // БИЗНЕС-МЕТОДЫ:

    /**
     * Проверяет, требует ли событие обработки (отправки в Kafka).
     * События в статусе NEW или FAILED (с допустимым количеством ретраев) требуют обработки.
     *
     * @param maxRetries максимальное допустимое количество попыток отправки
     * @return true если событие требует обработки, false если уже обработано или превышены ретраи
     */
    public boolean requiresProcessing(int maxRetries) {
        return ("NEW".equals(status) || ("FAILED".equals(status) && retryCount < maxRetries));
    }

    /**
     * Проверяет, является ли событие завершенным (успешно отправленным в Kafka).
     *
     * @return true если событие в статусе SENT, false в противном случае
     */
    public boolean isCompleted() {
        return "SENT".equals(status);
    }

    /**
     * Создает копию события с обновленным статусом PROCESSING.
     * Используется OutboxEventPublisher перед отправкой в Kafka.
     *
     * @return новое событие в статусе PROCESSING
     */
    public OutboxEvent markAsProcessing() {
        return new OutboxEvent(
                id,
                aggregateType,
                aggregateId,
                eventType,
                payload,
                correlationId,
                "PROCESSING",
                createdAt,
                processedAt,
                retryCount,
                errorMessage,
                LocalDateTime.now()
        );
    }

    /**
     * Создает копию события с обновленным статусом SENT.
     * Используется после успешной отправки в Kafka.
     *
     * @return новое событие в статусе SENT
     */
    public OutboxEvent markAsSent() {
        return new OutboxEvent(
                id,
                aggregateType,
                aggregateId,
                eventType,
                payload,
                correlationId,
                "SENT",
                createdAt,
                LocalDateTime.now(), // устанавливаем время обработки
                retryCount,
                null, // очищаем сообщение об ошибке
                LocalDateTime.now()
        );
    }

    /**
     * Создает копию события с обновленным статусом FAILED.
     * Используется при ошибке отправки в Kafka.
     *
     * @param errorMessage сообщение об ошибке для диагностики
     * @return новое событие в статусе FAILED с увеличенным счетчиком ретраев
     */
    public OutboxEvent markAsFailed(String errorMessage) {
        return new OutboxEvent(
                id,
                aggregateType,
                aggregateId,
                eventType,
                payload,
                correlationId,
                "FAILED",
                createdAt,
                processedAt,
                retryCount + 1, // увеличиваем счетчик попыток
                errorMessage,
                LocalDateTime.now()
        );
    }

    @Override
    public String toString() {
        return "OutboxEvent{" +
                "id=" + id +
                ", aggregateType='" + aggregateType + '\'' +
                ", aggregateId=" + aggregateId +
                ", eventType='" + eventType + '\'' +
                ", correlationId=" + correlationId +
                ", status='" + status + '\'' +
                ", retryCount=" + retryCount +
                ", createdAt=" + createdAt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OutboxEvent)) return false;
        OutboxEvent that = (OutboxEvent) o;
        return id != null && id.equals(that.id);
    }

    /**
     * Builder класс для удобного создания OutboxEvent.
     * Позволяет создавать события с различными комбинациями параметров.
     */
    public static class OutboxEventBuilder {
        private String aggregateType;
        private UUID aggregateId;
        private String eventType;
        private String payload;
        private UUID correlationId;
        private String status = "NEW";
        private Integer retryCount = 0;

        public OutboxEventBuilder aggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        public OutboxEventBuilder aggregateId(UUID aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public OutboxEventBuilder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public OutboxEventBuilder payload(String payload) {
            this.payload = payload;
            return this;
        }

        /**
         * Установка correlationId для трассировки распределенных транзакций.
         * Соответствует требованию 2.1 ТЗ по идемпотентности.
         */
        public OutboxEventBuilder correlationId(UUID correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public OutboxEventBuilder status(String status) {
            this.status = status;
            return this;
        }

        public OutboxEventBuilder retryCount(Integer retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        /**
         * Создает объект OutboxEvent с заданными параметрами.
         * Автоматически устанавливает createdAt в текущее время.
         */
        public OutboxEvent build() {
            OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, eventType, payload, correlationId);
            event.setStatus(status);
            event.setRetryCount(retryCount);
            return event;
        }
    }
}