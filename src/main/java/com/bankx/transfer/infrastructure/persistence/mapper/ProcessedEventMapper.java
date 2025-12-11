package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.ProcessedEvent;
import com.bankx.transfer.domain.model.ProcessedEventType;
import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Маппер для преобразования между доменной моделью ProcessedEvent и JPA сущностью ProcessedEventEntity.
 * Обеспечивает идемпотентность обработки Kafka событий путем отслеживания обработанных событий.
 * Каждое событие, полученное из Kafka, регистрируется с уникальным идентификатором (correlationId + eventType)
 * для предотвращения повторной обработки.
 *
 * Отвечает за:
 * - Преобразование доменной модели в сущность для сохранения в БД
 * - Восстановление доменной модели из сущности
 * - Обновление сущности при изменениях в доменной модели
 * - Обеспечение целостности данных между слоями
 *
 * Соответствует требованиям ТЗ по идемпотентности обработки событий (2.1).
 */
@Component
@Slf4j
public class ProcessedEventMapper {

    /**
     * Преобразует доменную модель ProcessedEvent в JPA сущность ProcessedEventEntity.
     * Используется при сохранении информации об обработанном событии в базу данных.
     * Гарантирует сохранение всех необходимых полей для обеспечения идемпотентности.
     *
     * @param event доменная модель обработанного события
     * @return JPA сущность для сохранения в таблицу processed_events
     * @throws IllegalArgumentException если event null или содержит некорректные данные
     */
    public ProcessedEventEntity toEntity(ProcessedEvent event) {
        if (event == null) {
            log.warn("Attempt to map null ProcessedEvent to entity");
            return null;
        }

        log.debug("Mapping ProcessedEvent to entity: correlationId={}, eventType={}, transferId={}",
                event.getCorrelationId(), event.getEventType(), event.getTransferId());

        try {
            return ProcessedEventEntity.builder()
                    .id(event.getId())
                    .eventId(event.getEventId())
                    .correlationId(event.getCorrelationId())
                    .eventType(event.getEventType()) // Используем enum напрямую
                    .processedAt(event.getProcessedAt())
                    .payload(event.getPayload())
                    .transferId(event.getTransferId()) // Добавляем transferId
                    .createdAt(event.getCreatedAt())
                    .build();

        } catch (Exception e) {
            log.error("Failed to map ProcessedEvent to entity: correlationId={}, eventType={}, error={}",
                    event.getCorrelationId(), event.getEventType(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map ProcessedEvent to entity", e);
        }
    }

    /**
     * Преобразует JPA сущность ProcessedEventEntity в доменную модель ProcessedEvent.
     * Используется при чтении данных из БД для проверки идемпотентности.
     * Гарантирует корректное восстановление доменной модели со всеми валидными полями.
     *
     * @param entity JPA сущность из таблицы processed_events
     * @return доменная модель ProcessedEvent
     * @throws IllegalArgumentException если entity null или содержит некорректные данные
     */
    public ProcessedEvent toDomain(ProcessedEventEntity entity) {
        if (entity == null) {
            log.warn("Attempt to map null ProcessedEventEntity to domain");
            return null;
        }

        log.debug("Mapping ProcessedEventEntity to domain: id={}, correlationId={}, transferId={}",
                entity.getId(), entity.getCorrelationId(), entity.getTransferId());

        try {
            return ProcessedEvent.builder()
                    .id(entity.getId())
                    .eventId(entity.getEventId())
                    .correlationId(entity.getCorrelationId())
                    .eventType(entity.getEventType()) // Используем enum напрямую
                    .processedAt(entity.getProcessedAt())
                    .payload(entity.getPayload())
                    .transferId(entity.getTransferId()) // Добавляем transferId
                    .createdAt(entity.getCreatedAt())
                    .build();

        } catch (Exception e) {
            log.error("Failed to map ProcessedEventEntity to domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map ProcessedEventEntity to domain", e);
        }
    }

    /**
     * Обновляет существующую JPA сущность из доменной модели.
     * Используется при обновлении записей об обработанных событиях.
     * Сохраняет неизменяемые поля (id, eventId, correlationId, eventType) и обновляет изменяемые.
     *
     * @param domain доменная модель с обновленными данными
     * @param entity существующая JPA сущность для обновления
     * @throws IllegalArgumentException если domain или entity null
     */
    public void updateEntityFromDomain(ProcessedEvent domain, ProcessedEventEntity entity) {
        if (domain == null) {
            throw new IllegalArgumentException("ProcessedEvent domain cannot be null for update");
        }
        if (entity == null) {
            throw new IllegalArgumentException("ProcessedEventEntity cannot be null for update");
        }

        log.debug("Updating ProcessedEventEntity from domain: id={}, correlationId={}, transferId={}",
                entity.getId(), entity.getCorrelationId(), domain.getTransferId());

        try {
            // Обновляем только изменяемые поля
            // Неизменяемые: id, eventId, correlationId, eventType, createdAt
            entity.setProcessedAt(domain.getProcessedAt());
            entity.setPayload(domain.getPayload());
            entity.setTransferId(domain.getTransferId()); // Обновляем transferId

            log.debug("Successfully updated ProcessedEventEntity from domain: id={}",
                    entity.getId());

        } catch (Exception e) {
            log.error("Failed to update ProcessedEventEntity from domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to update ProcessedEventEntity from domain", e);
        }
    }

    /**
     * Создает новый ProcessedEventEntity из доменной модели ProcessedEvent.
     * Упрощенная версия для быстрого создания сущности без дополнительной логики.
     *
     * @param event доменная модель обработанного события
     * @return новая JPA сущность
     */
    public ProcessedEventEntity createEntity(ProcessedEvent event) {
        return toEntity(event);
    }

    /**
     * Создает доменную модель ProcessedEvent из минимального набора данных.
     * Используется для создания событий при первой обработке.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции события
     * @param eventType тип обработанного события
     * @param payload полезная нагрузка события
     * @return доменная модель ProcessedEvent
     */
    public ProcessedEvent createDomain(String eventId, String correlationId,
                                       ProcessedEventType eventType, String payload) {
        return ProcessedEvent.builder()
                .eventId(eventId)
                .correlationId(correlationId)
                .eventType(eventType)
                .processedAt(java.time.LocalDateTime.now())
                .payload(payload)
                .createdAt(java.time.LocalDateTime.now())
                .build();
    }

    /**
     * Проверяет соответствие доменной модели и JPA сущности.
     * Используется для валидации данных перед сохранением.
     *
     * @param domain доменная модель
     * @param entity JPA сущность
     * @return true если данные соответствуют, false в противном случае
     */
    public boolean validateMapping(ProcessedEvent domain, ProcessedEventEntity entity) {
        if (domain == null && entity == null) {
            return true;
        }
        if (domain == null || entity == null) {
            return false;
        }

        return domain.getId().equals(entity.getId()) &&
                domain.getEventId().equals(entity.getEventId()) &&
                domain.getCorrelationId().equals(entity.getCorrelationId()) &&
                domain.getEventType().equals(entity.getEventType()) &&
                java.util.Objects.equals(domain.getTransferId(), entity.getTransferId());
    }

    /**
     * Создает копию сущности с обновленными данными.
     * Используется для создания истории изменений.
     *
     * @param source исходная сущность
     * @param updates обновленные данные
     * @return новая сущность с обновленными данными
     */
    public ProcessedEventEntity createCopyWithUpdates(ProcessedEventEntity source, ProcessedEvent updates) {
        if (source == null || updates == null) {
            return null;
        }

        // Используем builder для создания новой сущности на основе source
        ProcessedEventEntity copy = ProcessedEventEntity.builder()
                .id(source.getId())
                .eventId(source.getEventId())
                .correlationId(source.getCorrelationId())
                .eventType(source.getEventType())
                .processedAt(updates.getProcessedAt())
                .payload(updates.getPayload())
                .transferId(updates.getTransferId()) // Копируем transferId
                .createdAt(source.getCreatedAt())
                .build();

        return copy;
    }

    /**
     * Проверяет, является ли событие уже обработанным.
     * Использует eventId для определения уникальности.
     *
     * @param entity сущность для проверки
     * @return true если событие уже обработано, false в противном случае
     */
    public boolean isAlreadyProcessed(ProcessedEventEntity entity) {
        if (entity == null) {
            return false;
        }
        return entity.getProcessedAt() != null;
    }

    /**
     * Получает ключ идемпотентности для события.
     * Используется для создания уникального идентификатора события.
     *
     * @param correlationId идентификатор корреляции
     * @param eventType тип события
     * @return строковый ключ идемпотентности
     */
    public String getIdempotencyKey(String correlationId, ProcessedEventType eventType) {
        return String.format("%s:%s", correlationId, eventType.getValue());
    }
}