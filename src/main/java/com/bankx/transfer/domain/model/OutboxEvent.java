package com.bankx.transfer.domain.model;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Доменная модель для исходящих событий в рамках Outbox Pattern.
 * Представляет бизнес-события, которые являются частью Saga процесса трансферов.
 *
 * События содержат бизнес-семантику и используются в бизнес-процессах:
 * - DEBIT_REQUEST, CREDIT_REQUEST - бизнес-действия
 * - TRANSFER_COMPLETED, TRANSFER_FAILED - бизнес-результаты
 * - COMPENSATE_DEBIT - бизнес-компенсации
 *
 * Соответствует требованиям ТЗ по надежной доставке событий через Kafka.
 */
public class OutboxEvent {

    private Long id;
    private final String aggregateType;
    private final UUID aggregateId;
    private final String eventType;
    private final String payload;
    private final String correlationId; // Для трассировки распределенных транзакций согласно ТЗ
    private String status;
    private final LocalDateTime createdAt;
    private LocalDateTime processedAt;
    private Integer retryCount;
    private String errorMessage;
    private LocalDateTime updatedAt;

    /**
     * Конструктор для создания новых событий.
     * Используется при порождении бизнес-событий в Saga процессе.
     *
     * @param correlationId идентификатор корреляции для трассировки согласно требованию 2.1 ТЗ
     */
    public OutboxEvent(String aggregateType, UUID aggregateId, String eventType,
                       String payload, String correlationId) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.status = "NEW";
        this.retryCount = 0;
        this.createdAt = LocalDateTime.now();
    }

    /**
     * Полный конструктор для восстановления из persistence слоя.
     */
    public OutboxEvent(Long id, String aggregateType, UUID aggregateId, String eventType,
                       String payload, String correlationId, String status, LocalDateTime createdAt,
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
    }

    // БИЗНЕС-МЕТОДЫ (Domain Logic)

    /**
     * Пометить событие как обрабатываемое.
     * Вызывается перед попыткой отправки в Kafka.
     * Используется в OutboxEventPublisher для отслеживания состояния обработки.
     */
    public void markAsProcessing() {
        this.status = "PROCESSING";
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Пометить событие как успешно отправленное.
     * Вызывается после подтверждения доставки в Kafka.
     *
     * @param processedAt время успешной отправки для аудита и мониторинга
     */
    public void markAsSent(LocalDateTime processedAt) {
        this.status = "SENT";
        this.processedAt = processedAt;
        this.updatedAt = LocalDateTime.now();
        this.errorMessage = null; // Очищаем предыдущие ошибки при успешной отправке
    }

    /**
     * Пометить событие как неудачное и увеличить счетчик попыток.
     * Вызывается при ошибках отправки в Kafka.
     *
     * @param errorMessage описание ошибки для диагностики и мониторинга
     */
    public void markAsFailed(String errorMessage) {
        this.status = "FAILED";
        this.retryCount++;
        this.errorMessage = errorMessage;
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Проверить, можно ли повторно отправить событие.
     * Используется для ограничения бесконечных повторных попыток.
     *
     * @param maxRetries максимальное количество разрешенных попыток отправки
     * @return true если можно повторить отправку, false если превышен лимит попыток
     */
    public boolean canRetry(int maxRetries) {
        return "FAILED".equals(this.status) && this.retryCount < maxRetries;
    }

    /**
     * Проверить, является ли событие завершенным.
     * Завершенные события не требуют дальнейшей обработки.
     *
     * @return true если событие успешно отправлено и завершено
     */
    public boolean isCompleted() {
        return "SENT".equals(this.status);
    }

    /**
     * Проверить, требует ли событие обработки.
     * Новые и неудачные события требуют обработки.
     *
     * @return true если событие новое или неудачное (требует обработки)
     */
    public boolean requiresProcessing() {
        return "NEW".equals(this.status) || "FAILED".equals(this.status);
    }

    /**
     * Валидация статуса события.
     * Гарантирует, что статус соответствует допустимым значениям.
     *
     * @param status проверяемый статус
     * @return true если статус допустим
     */
    private boolean isValidStatus(String status) {
        return status != null &&
                (status.equals("NEW") || status.equals("PROCESSING") ||
                        status.equals("SENT") || status.equals("FAILED"));
    }

    // ГЕТТЕРЫ

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
    public String getCorrelationId() {
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

    // СЕТТЕРЫ (ограниченные, только для необходимых полей)

    /**
     * Сеттер для статуса с дополнительной валидацией.
     * Гарантирует целостность состояния события.
     */
    public void setStatus(String status) {
        if (!isValidStatus(status)) {
            throw new IllegalArgumentException("Invalid status: " + status +
                    ". Must be one of: NEW, PROCESSING, SENT, FAILED");
        }
        this.status = status;
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Сеттер для retryCount с валидацией.
     * Защищает от отрицательного количества попыток.
     */
    public void setRetryCount(Integer retryCount) {
        if (retryCount < 0) {
            throw new IllegalArgumentException("Retry count cannot be negative");
        }
        this.retryCount = retryCount;
        this.updatedAt = LocalDateTime.now();
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
        this.updatedAt = LocalDateTime.now();
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.updatedAt = LocalDateTime.now();
    }

    // EQUALS и HASHCODE (по ID)

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OutboxEvent)) return false;
        OutboxEvent that = (OutboxEvent) o;
        return id != null && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    // TO STRING (для логирования и отладки)

    @Override
    public String toString() {
        return "OutboxEvent{" +
                "id=" + id +
                ", aggregateType='" + aggregateType + '\'' +
                ", aggregateId=" + aggregateId +
                ", eventType='" + eventType + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", status='" + status + '\'' +
                ", retryCount=" + retryCount +
                ", createdAt=" + createdAt +
                '}';
    }

    // BUILDER PATTERN (для удобного создания)

    /**
     * Статический метод для создания builder'а.
     * Упрощает создание сложных объектов OutboxEvent.
     */
    public static OutboxEventBuilder builder() {
        return new OutboxEventBuilder();
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
        private String correlationId;
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
        public OutboxEventBuilder correlationId(String correlationId) {
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