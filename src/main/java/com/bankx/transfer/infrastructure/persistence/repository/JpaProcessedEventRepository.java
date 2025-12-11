package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.ProcessedEvent;
import com.bankx.transfer.domain.repository.ProcessedEventRepository;
import com.bankx.transfer.infrastructure.persistence.mapper.ProcessedEventMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Адаптер уровня инфраструктуры, который реализует доменный порт ProcessedEventRepository
 * поверх Spring Data JPA-репозитория (SpringDataProcessedEventRepository).
 */
@Repository
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class JpaProcessedEventRepository implements ProcessedEventRepository {

    private final SpringDataProcessedEventRepository jpa;
    private final ProcessedEventMapper mapper;

    @Override
    @Transactional
    public ProcessedEvent save(ProcessedEvent event) {
        log.debug("processed_event.save eventId={}, type={}, transferId={}",
                event.getEventId(), event.getEventType(), event.getTransferId());
        try {
            var saved = jpa.save(mapper.toEntity(event));
            jpa.flush(); // гарантируем проверку уникальности прямо здесь
            return mapper.toDomain(saved);
        } catch (org.springframework.dao.DataIntegrityViolationException dup) {
            log.debug("processed_event.duplicate eventId={}, type={}, transferId={}",
                    event.getEventId(), event.getEventType(), event.getTransferId());

            // 1) точный дубль по eventId
            var byEventId = findByEventId(event.getEventId());
            if (byEventId.isPresent()) return byEventId.get();

            // 2) дубль по (transferId, eventType)
            if (event.getTransferId() != null) {
                var byTransferIdAndEventType = jpa.findFirstByTransferIdAndEventType(event.getTransferId(), event.getEventType());
                if (byTransferIdAndEventType.isPresent()) return mapper.toDomain(byTransferIdAndEventType.get());
            }

            // 3) дубль по (correlationId, eventType)
            if (event.getCorrelationId() != null) {
                var byCorrelationIdAndEventType = jpa.findByCorrelationIdAndEventType(event.getCorrelationId(), event.getEventType());
                if (byCorrelationIdAndEventType.isPresent()) return mapper.toDomain(byCorrelationIdAndEventType.get());
            }

            // 4) на всякий случай — пробрасываем, если ничего не нашли
            throw dup;
        }
    }

    @Override
    public Optional<ProcessedEvent> findById(Long id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public Optional<ProcessedEvent> findByEventId(String eventId) {
        return jpa.findByEventId(eventId).map(mapper::toDomain);
    }

    @Override
    public boolean existsByEventId(String eventId) {
        return jpa.existsByEventId(eventId);
    }

    @Override
    public List<ProcessedEvent> findRecentByTransferId(UUID transferId, int limit) {
        int pageSize = Math.max(1, limit);
        return jpa.findByTransferIdOrderByCreatedAtDesc(transferId, PageRequest.of(0, pageSize))
                .stream()
                .map(mapper::toDomain)
                .toList();
    }

    @Override
    public List<ProcessedEvent> findAllByTransferIdOrderByCreatedAtAsc(UUID transferId) {
        return jpa.findByTransferIdOrderByCreatedAtAsc(transferId)
                .stream()
                .map(mapper::toDomain)
                .toList();
    }

    @Override
    @Transactional
    public void deleteOlderThan(LocalDateTime before) {
        jpa.deleteByCreatedAtBefore(before);
    }
}