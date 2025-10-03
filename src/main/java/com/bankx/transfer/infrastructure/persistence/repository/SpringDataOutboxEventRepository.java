package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.infrastructure.persistence.entity.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface SpringDataOutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    List<OutboxEventEntity> findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
            List<String> statuses, int maxRetries);

    List<OutboxEventEntity> findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
            List<String> statuses, int maxRetries, Pageable pageable);

    List<OutboxEventEntity> findByAggregateTypeAndAggregateId(String aggregateType, UUID aggregateId);

    List<OutboxEventEntity> findByEventType(String eventType);

    boolean existsByAggregateIdAndEventType(UUID aggregateId, String eventType);

    @Modifying
    @Query("DELETE FROM OutboxEventEntity o WHERE o.status = 'SENT' AND o.processedAt < :beforeDate")
    int deleteOldProcessedEvents(@Param("beforeDate") LocalDateTime beforeDate);

    List<OutboxEventEntity> findByStatus(String status);

    long countByStatus(String status);

    long countByEventType(String eventType);
}