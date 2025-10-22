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

/**
 * SPRING DATA JPA РЕПОЗИТОРИЙ ДЛЯ OUTBOX_EVENTS
 *
 * ОСОБЕННОСТИ SPRING DATA JPA:
 * - Автоматическая генерация SQL запросов из имен методов
 * - Поддержка пагинации, сортировки, проекций
 * - Минимальный boilerplate код
 * - Оптимизированные запросы к БД
 *
 * КАК РАБОТАЕТ:
 * 1. Spring Boot создает реализацию этого интерфейса во время выполнения
 * 2. По имени метода генерируется соответствующий JPQL/SQL запрос
 * 3. Аннотации @Query позволяют писать кастомные запросы
 * 4. @Modifying указывает на модифицирующие запросы (UPDATE/DELETE)
 */
@Repository
public interface SpringDataOutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    /**
     * АВТОМАТИЧЕСКИЙ ЗАПРОС ПО ИМЕНИ МЕТОДА:
     * SELECT * FROM outbox_events
     * WHERE status IN (:statuses) AND retry_count < :maxRetries
     * ORDER BY created_at ASC
     */
    List<OutboxEventEntity> findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
            List<String> statuses, int maxRetries);

    /**
     * ТОТ ЖЕ ЗАПРОС + ПАГИНАЦИЯ:
     * Добавляет LIMIT и OFFSET для batch обработки
     */
    List<OutboxEventEntity> findByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
            List<String> statuses, int maxRetries, Pageable pageable);

    /**
     * ПОИСК СОБЫТИЙ ДЛЯ КОНКРЕТНОГО АГРЕГАТА:
     * SELECT * FROM outbox_events
     * WHERE aggregate_type = :aggregateType AND aggregate_id = :aggregateId
     */
    List<OutboxEventEntity> findByAggregateTypeAndAggregateId(String aggregateType, UUID aggregateId);

    /**
     * ПОИСК ПО ТИПУ СОБЫТИЯ:
     * SELECT * FROM outbox_events WHERE event_type = :eventType
     * Используется для анализа бизнес-процессов
     */
    List<OutboxEventEntity> findByEventType(String eventType);

    /**
     * ПРОВЕРКА СУЩЕСТВОВАНИЯ ДУБЛИРУЮЩЕГО СОБЫТИЯ:
     * SELECT COUNT(*) > 0 FROM outbox_events
     * WHERE aggregate_id = :aggregateId AND event_type = :eventType
     *
     * Защита от дублирования событий для одного агрегата
     */
    boolean existsByAggregateIdAndEventType(UUID aggregateId, String eventType);

    /**
     * КАСТОМНЫЙ DELETE ЗАПРОС ДЛЯ ОЧИСТКИ:
     * DELETE FROM outbox_events
     * WHERE status = 'SENT' AND processed_at < :beforeDate
     *
     * @Modifying - указывает, что запрос изменяет данные
     * @Query - кастомный JPQL запрос
     * @Param - связывание параметров
     *
     * @return количество удаленных записей
     */
    @Modifying
    @Query("DELETE FROM OutboxEventEntity o WHERE o.status = 'SENT' AND o.processedAt < :beforeDate")
    int deleteOldProcessedEvents(@Param("beforeDate") LocalDateTime beforeDate);

    /**
     * МЕТОДЫ ДЛЯ СТАТИСТИКИ И МОНИТОРИНГА:
     * Spring Data автоматически генерирует COUNT запросы
     */
    List<OutboxEventEntity> findByStatus(String status);
    long countByStatus(String status);
    long countByEventType(String eventType);
}