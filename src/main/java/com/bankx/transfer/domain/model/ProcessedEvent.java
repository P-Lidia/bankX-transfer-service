package com.bankx.transfer.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Доменная модель обработанного события (ProcessedEvent).
 * Представляет факт, что входящее сообщение было обработано,
 * и предотвращает повторную обработку (идемпотентность).
 * * <p>Создание новых объектов осуществляется через фабричный метод
 * * {@link #newOf(String, UUID, ProcessedEventType)}, который:
 * * <ul>
 * *   <li>гарантирует валидацию обязательных полей (eventId, transferId, eventType);</li>
 * *   <li>устанавливает служебные поля {@code id} и {@code createdAt} в {@code null},
 * *       так как их заполняет база данных при сохранении;</li>
 * *   <li>повышает читаемость и предотвращает ошибки ручного вызова конструктора.</li>
 * * </ul>
 * * Конструктор открыт в основном для восстановления объекта из БД (маппинг в репозитории).</p>
 */

public class ProcessedEvent {

    private final Long id;
    private final String eventId;
    private final UUID transferId;
    private final ProcessedEventType eventType;
    private final Instant createdAt;

    public ProcessedEvent(Long id,
                          String eventId,
                          UUID transferId,
                          ProcessedEventType eventType,
                          Instant createdAt) {
        this.id = id;
        this.eventId = eventId;
        this.transferId = transferId;
        this.eventType = eventType;
        this.createdAt = createdAt;
    }
    public static ProcessedEvent newOf(String eventId, UUID transferId, ProcessedEventType type) {
        if (eventId == null || eventId.isBlank()) throw new IllegalArgumentException("eventId blank");
        java.util.Objects.requireNonNull(transferId, "transferId");
        java.util.Objects.requireNonNull(type, "eventType");
        return new ProcessedEvent(null, eventId, transferId, type, null);
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public UUID getTransferId() { return transferId; }
    public ProcessedEventType getEventType() { return eventType; }
    public Instant getCreatedAt() { return createdAt; }
}