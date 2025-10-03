package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.domain.repository.OutboxEventRepository;
import com.bankx.transfer.infrastructure.persistence.entity.OutboxEventEntity;
import com.bankx.transfer.infrastructure.persistence.mapper.OutboxEventMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Реализация порта OutboxEventRepository с использованием JPA.
 * Инфраструктурная реализация доменного интерфейса для работы с Outbox событиями.
 *
 * Отвечает за:
 * - Сохранение и извлечение событий в/из PostgreSQL
 * - Преобразование между доменной моделью и JPA сущностью
 * - Обеспечение транзакционности операций
 * - Логирование операций для observability
 */
@Repository
public class JpaOutboxEventRepository implements OutboxEventRepository {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxEventRepository.class);

    private final SpringDataOutboxEventRepository springDataOutboxEventRepository;
    private final OutboxEventMapper outboxEventMapper;

    public JpaOutboxEventRepository(SpringDataOutboxEventRepository springDataOutboxEventRepository,
                                    OutboxEventMapper outboxEventMapper) {
        this.springDataOutboxEventRepository = springDataOutboxEventRepository;
        this.outboxEventMapper = outboxEventMapper;
    }

    @Override
    @Transactional
    public OutboxEvent save(OutboxEvent event) {
        log.debug("Сохранение outbox события: aggregateType={}, eventType={}",
                event.getAggregateType(), event.getEventType());

        try {
            OutboxEventEntity entity = outboxEventMapper.toEntity(event);
            OutboxEventEntity savedEntity = springDataOutboxEventRepository.save(entity);
            OutboxEvent savedEvent = outboxEventMapper.toDomain(savedEntity);

            log.info("Outbox событие успешно сохранено: id={}, aggregateType={}, eventType={}",
                    savedEvent.getId(), savedEvent.getAggregateType(), savedEvent.getEventType());
            return savedEvent;

        } catch (Exception e) {
            log.error("Ошибка при сохранении outbox события: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось сохранить outbox событие", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<OutboxEvent> findById(Long id) {
        log.debug("Поиск outbox события по ID: {}", id);

        try {
            Optional<OutboxEventEntity> entity = springDataOutboxEventRepository.findById(id);
            Optional<OutboxEvent> result = entity.map(outboxEventMapper::toDomain);

            if (result.isPresent()) {
                log.debug("Найдено outbox событие: id={}, type={}", id, result.get().getEventType());
            } else {
                log.debug("Outbox событие с ID {} не найдено", id);
            }
            return result;

        } catch (Exception e) {
            log.error("Ошибка при поиске outbox события по ID {}: {}", id, e.getMessage(), e);
            throw new RuntimeException("Не удалось найти outbox событие", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> findUnprocessedEvents(int maxRetries) {
        log.debug("Поиск необработанных outbox событий с maxRetries={}", maxRetries);

        try {
            List<OutboxEventEntity> entities = springDataOutboxEventRepository
                    .findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
                            List.of("NEW", "FAILED"), maxRetries);

            List<OutboxEvent> result = entities.stream()
                    .map(outboxEventMapper::toDomain)
                    .collect(Collectors.toList());

            log.info("Найдено {} необработанных outbox событий", result.size());
            return result;

        } catch (Exception e) {
            log.error("Ошибка при поиске необработанных outbox событий: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось найти необработанные outbox события", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> findByAggregateTypeAndAggregateId(String aggregateType, UUID aggregateId) {
        log.debug("Поиск outbox событий по aggregateType={}, aggregateId={}", aggregateType, aggregateId);

        try {
            List<OutboxEventEntity> entities = springDataOutboxEventRepository
                    .findByAggregateTypeAndAggregateId(aggregateType, aggregateId);

            List<OutboxEvent> result = entities.stream()
                    .map(outboxEventMapper::toDomain)
                    .collect(Collectors.toList());

            log.debug("Найдено {} outbox событий для aggregateType={}, aggregateId={}",
                    result.size(), aggregateType, aggregateId);
            return result;

        } catch (Exception e) {
            log.error("Ошибка при поиске outbox событий по агрегату: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось найти outbox события по агрегату", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> findByEventType(String eventType) {
        log.debug("Поиск outbox событий по eventType={}", eventType);

        try {
            List<OutboxEventEntity> entities = springDataOutboxEventRepository.findByEventType(eventType);

            List<OutboxEvent> result = entities.stream()
                    .map(outboxEventMapper::toDomain)
                    .collect(Collectors.toList());

            log.debug("Найдено {} outbox событий с eventType={}", result.size(), eventType);
            return result;

        } catch (Exception e) {
            log.error("Ошибка при поиске outbox событий по eventType {}: {}", eventType, e.getMessage(), e);
            throw new RuntimeException("Не удалось найти outbox события по типу", e);
        }
    }

    @Override
    @Transactional
    public OutboxEvent update(OutboxEvent event) {
        log.debug("Обновление outbox события: id={}, status={}", event.getId(), event.getStatus());

        try {
            // Для обновления используем тот же save метод Spring Data
            OutboxEventEntity entity = outboxEventMapper.toEntity(event);
            OutboxEventEntity updatedEntity = springDataOutboxEventRepository.save(entity);
            OutboxEvent updatedEvent = outboxEventMapper.toDomain(updatedEntity);

            log.info("Outbox событие успешно обновлено: id={}, status={}",
                    updatedEvent.getId(), updatedEvent.getStatus());
            return updatedEvent;

        } catch (Exception e) {
            log.error("Ошибка при обновлении outbox события id {}: {}", event.getId(), e.getMessage(), e);
            throw new RuntimeException("Не удалось обновить outbox событие", e);
        }
    }

    @Override
    @Transactional
    public void delete(OutboxEvent event) {
        log.debug("Удаление outbox события: id={}", event.getId());

        try {
            OutboxEventEntity entity = outboxEventMapper.toEntity(event);
            springDataOutboxEventRepository.delete(entity);

            log.info("Outbox событие успешно удалено: id={}", event.getId());

        } catch (Exception e) {
            log.error("Ошибка при удалении outbox события id {}: {}", event.getId(), e.getMessage(), e);
            throw new RuntimeException("Не удалось удалить outbox событие", e);
        }
    }

    @Override
    @Transactional
    public void deleteOldProcessedEvents(LocalDateTime beforeDate) {
        log.debug("Удаление старых обработанных outbox событий до: {}", beforeDate);

        try {
            // 🔥 ИСПРАВЛЕННЫЙ ВЫЗОВ - используем правильное имя метода
            int deletedCount = springDataOutboxEventRepository.deleteOldProcessedEvents(beforeDate);

            log.info("Удалено {} старых обработанных outbox событий", deletedCount);

        } catch (Exception e) {
            log.error("Ошибка при удалении старых outbox событий: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось удалить старые outbox события", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public boolean existsByAggregateIdAndEventType(UUID aggregateId, String eventType) {
        log.debug("Проверка существования outbox события для aggregateId={} и eventType={}",
                aggregateId, eventType);

        try {
            boolean exists = springDataOutboxEventRepository
                    .existsByAggregateIdAndEventType(aggregateId, eventType);

            log.debug("Outbox событие для aggregateId={} и eventType={} существует: {}",
                    aggregateId, eventType, exists);
            return exists;

        } catch (Exception e) {
            log.error("Ошибка при проверке существования outbox события: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось проверить существование outbox события", e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> findUnprocessedEventsWithLimit(int maxRetries, int limit) {
        log.debug("Поиск необработанных outbox событий с maxRetries={}, limit={}", maxRetries, limit);

        try {
            Pageable pageable = PageRequest.of(0, limit);
            List<OutboxEventEntity> entities = springDataOutboxEventRepository
                    .findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
                            List.of("NEW", "FAILED"), maxRetries, pageable);

            List<OutboxEvent> result = entities.stream()
                    .map(outboxEventMapper::toDomain)
                    .collect(Collectors.toList());

            log.debug("Найдено {} необработанных outbox событий (limit={})", result.size(), limit);
            return result;

        } catch (Exception e) {
            log.error("Ошибка при поиске необработанных outbox событий с лимитом: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось найти необработанные outbox события с лимитом", e);
        }
    }

    @Override
    @Transactional
    public void markAsProcessing(Long eventId) {
        log.debug("Пометка outbox события как обрабатываемого: id={}", eventId);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsProcessing();
                springDataOutboxEventRepository.save(event);
                log.debug("Outbox событие помечено как PROCESSING: id={}", eventId);
            } else {
                log.warn("Outbox событие с ID {} не найдено для пометки как PROCESSING", eventId);
            }
        } catch (Exception e) {
            log.error("Ошибка при пометке outbox события как обрабатываемого: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось пометить outbox событие как обрабатываемое", e);
        }
    }

    @Override
    @Transactional
    public void markAsSent(Long eventId) {
        log.debug("Пометка outbox события как отправленного: id={}", eventId);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsSent(LocalDateTime.now());
                springDataOutboxEventRepository.save(event);
                log.debug("Outbox событие помечено как SENT: id={}", eventId);
            } else {
                log.warn("Outbox событие с ID {} не найдено для пометки как SENT", eventId);
            }
        } catch (Exception e) {
            log.error("Ошибка при пометке outbox события как отправленного: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось пометить outbox событие как отправленное", e);
        }
    }

    @Override
    @Transactional
    public void markAsFailed(Long eventId, String errorMessage) {
        log.debug("Пометка outbox события как неудачного: id={}, error={}", eventId, errorMessage);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsFailed(errorMessage);
                springDataOutboxEventRepository.save(event);
                log.debug("Outbox событие помечено как FAILED: id={}", eventId);
            } else {
                log.warn("Outbox событие с ID {} не найдено для пометки как FAILED", eventId);
            }
        } catch (Exception e) {
            log.error("Ошибка при пометке outbox события как неудачного: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось пометить outbox событие как неудачное", e);
        }
    }

    // ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ ДЛЯ СТАТИСТИКИ И МОНИТОРИНГА:
    /**
     * Получить количество событий по статусу.
     * Используется для мониторинга и метрик Prometheus.
     */
    @Transactional(readOnly = true)
    public long countByStatus(String status) {
        log.debug("Получение количества outbox событий со статусом: {}", status);

        try {
            long count = springDataOutboxEventRepository.countByStatus(status);
            log.debug("Количество outbox событий со статусом {}: {}", status, count);
            return count;
        } catch (Exception e) {
            log.error("Ошибка при получении количества outbox событий по статусу: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось получить количество outbox событий", e);
        }
    }

    /**
     * Получить количество событий по типу события.
     * Используется для аналитики и мониторинга бизнес-процессов.
     */
    @Transactional(readOnly = true)
    public long countByEventType(String eventType) {
        log.debug("Получение количества outbox событий с типом: {}", eventType);

        try {
            long count = springDataOutboxEventRepository.countByEventType(eventType);
            log.debug("Количество outbox событий с типом {}: {}", eventType, count);
            return count;
        } catch (Exception e) {
            log.error("Ошибка при получении количества outbox событий по типу: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось получить количество outbox событий по типу", e);
        }
    }

    /**
     * Получить статистику по событиям.
     * Используется для дашбордов мониторинга и алертинга.
     */
    @Transactional(readOnly = true)
    public OutboxEventStatistics getStatistics() {
        log.debug("Получение статистики по outbox событиям");

        try {
            long totalEvents = springDataOutboxEventRepository.count();
            long newEvents = springDataOutboxEventRepository.countByStatus("NEW");
            long failedEvents = springDataOutboxEventRepository.countByStatus("FAILED");
            long sentEvents = springDataOutboxEventRepository.countByStatus("SENT");
            long processingEvents = springDataOutboxEventRepository.countByStatus("PROCESSING");

            OutboxEventStatistics statistics = new OutboxEventStatistics(
                    totalEvents, newEvents, failedEvents, sentEvents, processingEvents
            );

            log.debug("Статистика outbox событий: {}", statistics);
            return statistics;

        } catch (Exception e) {
            log.error("Ошибка при получении статистики outbox событий: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось получить статистику outbox событий", e);
        }
    }

    /**
     * Внутренний класс для статистики outbox событий.
     * Содержит агрегированные данные для мониторинга и отчетности.
     */
    public static class OutboxEventStatistics {
        private final long totalEvents;
        private final long newEvents;
        private final long failedEvents;
        private final long sentEvents;
        private final long processingEvents;

        /**
         * Конструктор статистики outbox событий.
         */
        public OutboxEventStatistics(long totalEvents, long newEvents, long failedEvents,
                                     long sentEvents, long processingEvents) {
            this.totalEvents = totalEvents;
            this.newEvents = newEvents;
            this.failedEvents = failedEvents;
            this.sentEvents = sentEvents;
            this.processingEvents = processingEvents;
        }

        // ГЕТТЕРЫ для доступа к статистическим данным:

        public long getTotalEvents() { return totalEvents; }
        public long getNewEvents() { return newEvents; }
        public long getFailedEvents() { return failedEvents; }
        public long getSentEvents() { return sentEvents; }
        public long getProcessingEvents() { return processingEvents; }

        /**
         * Строковое представление статистики для логирования и отладки.
         */
        @Override
        public String toString() {
            return String.format(
                    "OutboxEventStatistics{total=%d, new=%d, failed=%d, sent=%d, processing=%d}",
                    totalEvents, newEvents, failedEvents, sentEvents, processingEvents
            );
        }
    }
}