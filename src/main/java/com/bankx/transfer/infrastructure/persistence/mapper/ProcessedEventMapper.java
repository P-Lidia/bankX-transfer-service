package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.ProcessedEvent;
import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Маппер для преобразования между доменной моделью ProcessedEvent и JPA сущностью ProcessedEventEntity.
 */
@Component
@Slf4j
public class ProcessedEventMapper {

    public ProcessedEventEntity toEntity(ProcessedEvent processedEvent) {
        if (processedEvent == null) {
            return null;
        }
        log.debug("Mapping ProcessedEvent domain to Entity - id: {}", processedEvent.getId());

        return ProcessedEventEntity.builder()
                .id(processedEvent.getId())
                .eventId(processedEvent.getEventId())
                .transferId(processedEvent.getTransferId())
                .eventType(processedEvent.getEventType())
                // createdAt будет установлен автоматически через @CreationTimestamp и @PrePersist
                .build();
    }

    public ProcessedEvent toDomain(ProcessedEventEntity entity) {
        if (entity == null) {
            return null;
        }
        log.debug("Mapping ProcessedEventEntity to ProcessedEvent - id: {}", entity.getId());

        return new ProcessedEvent(
                entity.getId(),
                entity.getEventId(),
                entity.getTransferId(),
                entity.getEventType(),
                entity.getCreatedAt() == null ? null :
                        entity.getCreatedAt().atOffset(ZoneOffset.UTC).toInstant()
        );
    }
}