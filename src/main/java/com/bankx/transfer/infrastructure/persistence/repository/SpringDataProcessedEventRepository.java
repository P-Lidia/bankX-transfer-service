package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.ProcessedEventType;
import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA-репозиторий для таблицы processed_events.
 * Используется адаптером JpaProcessedEventRepository, который реализует доменный порт.
 */
public interface SpringDataProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    Optional<ProcessedEventEntity> findByEventId(String eventId);

    boolean existsByEventId(String eventId);

    List<ProcessedEventEntity> findByTransferIdOrderByCreatedAtAsc(UUID transferId);

    List<ProcessedEventEntity> findByTransferIdOrderByCreatedAtDesc(UUID transferId, Pageable pageable);

    List<ProcessedEventEntity> findByTransferIdAndEventType(UUID transferId, ProcessedEventType eventType);

    long deleteByCreatedAtBefore(LocalDateTime before);
}