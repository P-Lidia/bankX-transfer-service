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
    private final UUID correlationId; // Изменено с String на UUID
    private final String status;
    private final LocalDateTime createdAt;
    private final LocalDateTime processedAt;
    private final Integer retryCount;
    private final String errorMessage;
    private final LocalDateTime updatedAt;

    /**
     * Основной конструктор для создания доменной модели OutboxEvent.
     * Используется маппером при загрузке из БД и в тестовых сценариях.
     *
     * @param id уникальный идентификатор события (генерируется БД)
     * @param aggregateType тип агрегата-источника (например, "Transfer")
     * @param aggregateId идентификатор агрегата-источника
     * @param eventType тип бизнес-события (DEBIT_REQUEST, CREDIT_REQUEST и т.д.)
     * @param payload данные события в формате JSON
     * @param correlationId идентификатор корреляции для трассировки
     * @param status текущий статус обработки события
     * @param createdAt время создания события
     * @param processedAt время успешной обработки (отправки в Kafka)
     * @param retryCount количество попыток отправки
     * @param errorMessage сообщение об ошибке (при наличии)
     * @param updatedAt время последнего обновления
     */
    public OutboxEvent(Long id, String aggregateType, UUID aggregateId,
                       String eventType, String payload, UUID correlationId, // Изменено на UUID
                       String status, LocalDateTime createdAt,
                       LocalDateTime processedAt, Integer retryCount,
                       String errorMessage, LocalDateTime updatedAt) {
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

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}