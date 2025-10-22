package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.infrastructure.persistence.entity.OutboxEventEntity;
import org.springframework.stereotype.Component;

/**
 * Маппер для преобразования между доменной моделью OutboxEvent и JPA сущностью OutboxEventEntity.
 */
@Component
public class OutboxEventMapper {

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

        return entity;
    }

    public OutboxEvent toDomain(OutboxEventEntity entity) {
        if (entity == null) {
            return null;
        }

        // конструктор с correlationId
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
    }
}