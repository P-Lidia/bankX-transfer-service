package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.ProcessedEventType;
import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA-репозиторий для таблицы processed_events.
 * Используется адаптером JpaProcessedEventRepository, который реализует доменный порт.
 */
@Repository
public interface SpringDataProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    Optional<ProcessedEventEntity> findByEventId(String eventId);

    boolean existsByEventId(String eventId);

    List<ProcessedEventEntity> findByTransferIdOrderByCreatedAtAsc(UUID transferId);

    List<ProcessedEventEntity> findByTransferIdOrderByCreatedAtDesc(UUID transferId, Pageable pageable);

    List<ProcessedEventEntity> findByTransferIdAndEventType(UUID transferId, ProcessedEventType eventType);

    long deleteByCreatedAtBefore(LocalDateTime before);

    /**
     * Проверяет существование записи по transferId и eventType.
     * Используется для идемпотентной обработки событий перевода.
     */
    boolean existsByTransferIdAndEventType(UUID transferId, ProcessedEventType eventType);

    /**
     * Находит запись по transferId и eventType.
     * Используется при обработке дубликатов событий.
     */
    Optional<ProcessedEventEntity> findFirstByTransferIdAndEventType(UUID transferId, ProcessedEventType eventType);

    /**
     * Находит записи по correlationId и eventType.
     * Используется для альтернативной проверки идемпотентности.
     */
    Optional<ProcessedEventEntity> findByCorrelationIdAndEventType(String correlationId, ProcessedEventType eventType);

    /**
     * Проверяет существование записи по correlationId и eventType.
     */
    boolean existsByCorrelationIdAndEventType(String correlationId, ProcessedEventType eventType);
}