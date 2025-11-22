package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.infrastructure.persistence.entity.OutboxEventEntity;
import org.springframework.stereotype.Component;

/**
 * Маппер для преобразования между доменной моделью OutboxEvent и JPA сущностью OutboxEventEntity.
 * Реализует преобразование данных между слоем домена и слоем инфраструктуры.
 *
 * Основные функции:
 * - Преобразование доменной модели в JPA сущность для сохранения в БД
 * - Преобразование JPA сущности в доменную модель для использования в бизнес-логике
 * - Обновление существующей JPA сущности из доменной модели
 *
 * Гарантирует согласованность данных между доменной моделью и persistence слоем.
 */
@Component
public class OutboxEventMapper {

    /**
     * Преобразует доменную модель OutboxEvent в JPA сущность OutboxEventEntity.
     * Используется при сохранении событий в базу данных.
     *
     * @param domain доменная модель события
     * @return JPA сущность для сохранения в БД, или null если domain равен null
     */
    public OutboxEventEntity toEntity(OutboxEvent domain) {
        if (domain == null) {
            return null;
        }

        OutboxEventEntity entity = new OutboxEventEntity();
        entity.setId(domain.getId());
        entity.setAggregateType(domain.getAggregateType());
        entity.setAggregateId(domain.getAggregateId());
        entity.setEventType(domain.getEventType());
        entity.setPayload(domain.getPayload());
        entity.setCorrelationId(domain.getCorrelationId());
        entity.setStatus(domain.getStatus());
        entity.setCreatedAt(domain.getCreatedAt());
        entity.setProcessedAt(domain.getProcessedAt());
        entity.setRetryCount(domain.getRetryCount());
        entity.setErrorMessage(domain.getErrorMessage());
        entity.setUpdatedAt(domain.getUpdatedAt());

        return entity;
    }

    /**
     * Преобразует JPA сущность OutboxEventEntity в доменную модель OutboxEvent.
     * Используется при загрузке событий из базы данных для бизнес-обработки.
     *
     * @param entity JPA сущность из БД
     * @return доменная модель события, или null если entity равен null
     */
    public OutboxEvent toDomain(OutboxEventEntity entity) {
        if (entity == null) {
            return null;
        }

        // Используем конструктор с correlationId
        return new OutboxEvent(
                entity.getId(),
                entity.getAggregateType(),
                entity.getAggregateId(),
                entity.getEventType(),
                entity.getPayload(),
                entity.getCorrelationId(),
                entity.getStatus(),
                entity.getCreatedAt(),
                entity.getProcessedAt(),
                entity.getRetryCount(),
                entity.getErrorMessage(),
                entity.getUpdatedAt()
        );
    }

    /**
     * Обновляет поля существующей JPA сущности из доменной модели.
     * Используется при обновлении событий в базе данных.
     * Не обновляет immutable поля (id, createdAt).
     *
     * @param domain доменная модель с обновленными данными
     * @param entity существующая JPA сущность для обновления
     */
    public void updateEntityFromDomain(OutboxEvent domain, OutboxEventEntity entity) {
        if (domain == null || entity == null) {
            return;
        }

        // ID и createdAt не обновляются - они immutable
        entity.setAggregateType(domain.getAggregateType());
        entity.setAggregateId(domain.getAggregateId());
        entity.setEventType(domain.getEventType());
        entity.setPayload(domain.getPayload());
        entity.setCorrelationId(domain.getCorrelationId());
        entity.setStatus(domain.getStatus());
        entity.setProcessedAt(domain.getProcessedAt());
        entity.setRetryCount(domain.getRetryCount());
        entity.setErrorMessage(domain.getErrorMessage());
        entity.setUpdatedAt(domain.getUpdatedAt());
    }
}