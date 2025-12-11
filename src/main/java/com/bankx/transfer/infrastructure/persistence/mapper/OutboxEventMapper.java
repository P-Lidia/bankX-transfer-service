package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.infrastructure.persistence.entity.OutboxEventEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Маппер для преобразования между доменной моделью OutboxEvent и JPA сущностью OutboxEventEntity.
 * Отвечает за корректное преобразование типов данных между слоями.
 * Сохраняет целостность данных при миграции между доменной моделью и persistence слоем.
 *
 * Реализует паттерн Data Mapper для разделения ответственности между
 * бизнес-логикой и инфраструктурой хранения данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventMapper {

    /**
     * Преобразует JPA сущность в доменную модель.
     * Используется при чтении данных из БД для бизнес-логики.
     * Гарантирует, что все обязательные поля будут инициализированы.
     *
     * @param entity JPA сущность из таблицы outbox_events
     * @return доменная модель OutboxEvent
     * @throws IllegalArgumentException если entity null или содержит некорректные данные
     */
    public OutboxEvent toDomain(OutboxEventEntity entity) {
        if (entity == null) {
            log.warn("Attempt to map null OutboxEventEntity to domain");
            return null;
        }

        log.debug("Mapping OutboxEventEntity to domain: id={}, eventType={}",
                entity.getId(), entity.getEventType());

        try {
            OutboxEvent domain = new OutboxEvent(
                    entity.getId(),
                    entity.getAggregateType(),
                    entity.getAggregateId(),
                    entity.getEventType(),
                    entity.getPayload(),
                    entity.getCorrelationId(),
                    entity.getStatus(),
                    entity.getErrorMessage(),
                    entity.getProcessedAt(),
                    entity.getRetryCount(),
                    entity.getCreatedAt(),
                    entity.getUpdatedAt()
            );

            log.debug("Successfully mapped OutboxEventEntity to domain: id={}", entity.getId());
            return domain;

        } catch (Exception e) {
            log.error("Failed to map OutboxEventEntity to domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map OutboxEventEntity to domain", e);
        }
    }

    /**
     * Преобразует доменную модель в JPA сущность.
     * Используется при сохранении данных в БД.
     * Гарантирует соответствие структуры таблице outbox_events.
     *
     * @param domain доменная модель OutboxEvent
     * @return JPA сущность для сохранения в таблицу outbox_events
     * @throws IllegalArgumentException если domain null или содержит некорректные данные
     */
    public OutboxEventEntity toEntity(OutboxEvent domain) {
        if (domain == null) {
            log.warn("Attempt to map null OutboxEvent to entity");
            return null;
        }

        log.debug("Mapping OutboxEvent to entity: aggregateType={}, eventType={}",
                domain.getAggregateType(), domain.getEventType());

        try {
            OutboxEventEntity entity = new OutboxEventEntity();
            entity.setId(domain.getId());
            entity.setAggregateType(domain.getAggregateType());
            entity.setAggregateId(domain.getAggregateId());
            entity.setEventType(domain.getEventType());
            entity.setPayload(domain.getPayload());
            entity.setCorrelationId(domain.getCorrelationId());
            entity.setStatus(domain.getStatus());
            entity.setErrorMessage(domain.getErrorMessage());
            entity.setProcessedAt(domain.getProcessedAt());
            entity.setRetryCount(domain.getRetryCount());
            entity.setCreatedAt(domain.getCreatedAt());
            entity.setUpdatedAt(domain.getUpdatedAt());

            log.debug("Successfully mapped OutboxEvent to entity: aggregateType={}, eventType={}",
                    domain.getAggregateType(), domain.getEventType());
            return entity;

        } catch (Exception e) {
            log.error("Failed to map OutboxEvent to entity: aggregateType={}, eventType={}, error={}",
                    domain.getAggregateType(), domain.getEventType(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map OutboxEvent to entity", e);
        }
    }

    /**
     * Обновляет существующую JPA сущность из доменной модели.
     * Используется при обновлении событий в outbox таблице.
     * Сохраняет неизменяемые поля (id, createdAt) и обновляет изменяемые.
     *
     * @param domain доменная модель с обновленными данными
     * @param entity существующая JPA сущность для обновления
     * @throws IllegalArgumentException если domain или entity null
     */
    public void updateEntityFromDomain(OutboxEvent domain, OutboxEventEntity entity) {
        if (domain == null) {
            throw new IllegalArgumentException("OutboxEvent domain cannot be null for update");
        }
        if (entity == null) {
            throw new IllegalArgumentException("OutboxEventEntity cannot be null for update");
        }

        log.debug("Updating OutboxEventEntity from domain: id={}, eventType={}",
                entity.getId(), domain.getEventType());

        try {
            // Обновляем только изменяемые поля
            // Неизменяемые: id, aggregateType, aggregateId, eventType, payload, correlationId, createdAt
            entity.setStatus(domain.getStatus());
            entity.setErrorMessage(domain.getErrorMessage());
            entity.setProcessedAt(domain.getProcessedAt());
            entity.setRetryCount(domain.getRetryCount());
            entity.setUpdatedAt(domain.getUpdatedAt());

            log.debug("Successfully updated OutboxEventEntity from domain: id={}", entity.getId());

        } catch (Exception e) {
            log.error("Failed to update OutboxEventEntity from domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to update OutboxEventEntity from domain", e);
        }
    }

    /**
     * Копирует данные из одной сущности в другую (клонирование).
     * Используется для создания новых записей на основе существующих.
     * Не копирует идентификатор (id) и временные метки.
     *
     * @param source исходная сущность для копирования
     * @param target целевая сущность для заполнения
     */
    public void copyEntityData(OutboxEventEntity source, OutboxEventEntity target) {
        if (source == null || target == null) {
            log.warn("Attempt to copy null entities");
            return;
        }

        target.setAggregateType(source.getAggregateType());
        target.setAggregateId(source.getAggregateId());
        target.setEventType(source.getEventType());
        target.setPayload(source.getPayload());
        target.setCorrelationId(source.getCorrelationId());
        target.setStatus(source.getStatus());
        target.setErrorMessage(source.getErrorMessage());
        target.setProcessedAt(source.getProcessedAt());
        target.setRetryCount(source.getRetryCount());
        // createdAt и updatedAt будут установлены при сохранении
    }

    /**
     * Проверяет соответствие доменной модели и JPA сущности.
     * Используется для валидации данных перед сохранением.
     *
     * @param domain доменная модель
     * @param entity JPA сущность
     * @return true если данные соответствуют, false в противном случае
     */
    public boolean validateMapping(OutboxEvent domain, OutboxEventEntity entity) {
        if (domain == null && entity == null) {
            return true;
        }
        if (domain == null || entity == null) {
            return false;
        }

        return domain.getId().equals(entity.getId()) &&
                domain.getAggregateType().equals(entity.getAggregateType()) &&
                domain.getAggregateId().equals(entity.getAggregateId()) &&
                domain.getEventType().equals(entity.getEventType()) &&
                domain.getPayload().equals(entity.getPayload()) &&
                domain.getCorrelationId().equals(entity.getCorrelationId());
    }
}