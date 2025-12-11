package com.bankx.transfer.domain.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Доменная модель для отслеживания обработанных событий.
 * Используется для обеспечения идемпотентности Kafka-консьюмеров.
 * Каждое событие, полученное из Kafka, регистрируется в системе
 * для предотвращения повторной обработки.
 * Отвечает за:
 * - Хранение информации об обработанных событиях
 * - Обеспечение идемпотентности через уникальные идентификаторы
 * - Поддержание истории обработки событий
 *
 * Соответствует требованиям ТЗ по идемпотентности (2.1).
 */
@Getter
@Setter
@Builder
public class ProcessedEvent {

    /**
     * Уникальный идентификатор записи в базе данных.
     * Генерируется автоматически при сохранении.
     * Используется для внутренней идентификации записи.
     */
    private UUID id;

    /**
     * Уникальный идентификатор события из Kafka.
     * Используется для прямой проверки идемпотентности.
     * Если событие с таким eventId уже обработано, оно игнорируется.
     */
    private String eventId;

    /**
     * Идентификатор корреляции события из Kafka.
     * Используется в сочетании с eventType для составного ключа идемпотентности.
     * Остается неизменным на протяжении всей Saga.
     */
    private String correlationId;

    /**
     * Тип обработанного события.
     * Определяет категорию события и его обработчик.
     * Используется в сочетании с correlationId для идемпотентности.
     */
    private ProcessedEventType eventType;

    /**
     * Дата и время обработки события.
     * Устанавливается в момент сохранения записи в БД.
     * Используется для аудита и мониторинга задержек обработки.
     */
    private LocalDateTime processedAt;

    /**
     * Полезная нагрузка события в формате JSON.
     * Содержит исходные данные события для отладки и аудита.
     * Опциональное поле, может быть null для экономии места.
     */
    private String payload;

    /**
     * Идентификатор перевода, связанного с событием.
     * Опциональное поле, присутствует только для событий, связанных с переводами.
     */
    private UUID transferId;

    /**
     * Дата и время создания записи.
     * Устанавливается автоматически при первом сохранении.
     * Используется для очистки старых записей по политике retention.
     */
    private LocalDateTime createdAt;

    /**
     * Создает новую запись об обработанном событии.
     * Упрощенный конструктор для частого случая - обработка события с transferId.
     *
     * @param eventId уникальный идентификатор события
     * @param transferId идентификатор перевода
     * @param eventType тип события
     * @return новый объект ProcessedEvent
     */
    public static ProcessedEvent newOf(String eventId, UUID transferId, ProcessedEventType eventType) {
        return ProcessedEvent.builder()
                .eventId(eventId)
                .correlationId(transferId.toString())
                .transferId(transferId) // Добавляем transferId
                .eventType(eventType)
                .processedAt(LocalDateTime.now())
                .createdAt(LocalDateTime.now())
                .build();
    }

    /**
     * Создает новую запись об обработанном событии с полным набором данных.
     * Используется для событий, не связанных с переводами.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции
     * @param eventType тип события
     * @param payload полезная нагрузка
     * @return новый объект ProcessedEvent
     */
    public static ProcessedEvent newOf(String eventId, String correlationId,
                                       ProcessedEventType eventType, String payload) {
        return ProcessedEvent.builder()
                .eventId(eventId)
                .correlationId(correlationId)
                .eventType(eventType)
                .processedAt(LocalDateTime.now())
                .payload(payload)
                .createdAt(LocalDateTime.now())
                .build();
    }

    /**
     * Проверяет, является ли запись валидной для идемпотентности.
     * Валидная запись должна иметь eventId или сочетание correlationId и eventType.
     *
     * @return true если запись может использоваться для проверки идемпотентности
     */
    public boolean isValidForIdempotency() {
        return eventId != null || (correlationId != null && eventType != null);
    }

    /**
     * Генерирует ключ идемпотентности на основе полей записи.
     * Используется для быстрой проверки в кеше или индексе.
     *
     * @return строковый ключ идемпотентности
     */
    public String generateIdempotencyKey() {
        if (eventId != null) {
            return "eventId:" + eventId;
        }
        if (correlationId != null && eventType != null) {
            return correlationId + ":" + eventType.getValue();
        }
        throw new IllegalStateException("Cannot generate idempotency key from incomplete data");
    }

    /**
     * Проверяет, является ли запись устаревшей.
     * Устаревшие записи могут быть удалены по политике очистки.
     * По умолчанию считается, что записи старше 30 дней устарели.
     *
     * @return true если запись старше 30 дней
     */
    public boolean isStale() {
        if (processedAt == null) {
            return false;
        }
        return processedAt.isBefore(LocalDateTime.now().minusDays(30));
    }

    @Override
    public String toString() {
        return String.format("ProcessedEvent[id=%s, eventId=%s, correlationId=%s, eventType=%s, processedAt=%s, transferId=%s]",
                id, eventId, correlationId, eventType, processedAt, transferId);
    }
}