package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Spring Data JPA репозиторий для сущности ProcessedEventEntity.
 * Предоставляет операции для работы с таблицей processed_events.
 *
 * Основная задача - обеспечение идемпотентности обработки событий Kafka
 * через отслеживание уже обработанных event_id.
 */
@Repository
public interface SpringDataProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    /**
     * Проверить существование записи по идентификатору события.
     * Используется для проверки идемпотентности - если событие уже обработано,
     * его не нужно обрабатывать повторно.
     *
     * @param eventId уникальный идентификатор события
     * @return true если событие уже обработано, false если нет
     */
    boolean existsByEventId(String eventId);

    /**
     * Найти запись по идентификатору события.
     * Используется для отладки и мониторинга.
     *
     * @param eventId идентификатор события
     * @return Optional с найденной записью
     */
    Optional<ProcessedEventEntity> findByEventId(String eventId);

    /**
     * Удалить старые обработанные события.
     * Используется для периодической очистки таблицы от устаревших записей.
     *
     * @param beforeDate дата, ранее которой записи считаются старыми
     * @return количество удаленных записей
     */
    @Modifying
    @Query("DELETE FROM ProcessedEventEntity p WHERE p.createdAt < :beforeDate")
    int deleteByCreatedAtBefore(@Param("beforeDate") LocalDateTime beforeDate);

    /**
     * Удалить запись по идентификатору события.
     * Используется для ручного управления в тестах или админских операциях.
     *
     * @param eventId идентификатор события для удаления
     */
    @Modifying
    @Query("DELETE FROM ProcessedEventEntity p WHERE p.eventId = :eventId")
    void deleteByEventId(@Param("eventId") String eventId);

    /**
     * Получить количество записей старше указанной даты.
     * Используется для мониторинга и метрик.
     *
     * @param beforeDate дата для фильтрации
     * @return количество старых записей
     */
    long countByCreatedAtBefore(LocalDateTime beforeDate);
}