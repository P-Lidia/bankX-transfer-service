package com.bankx.transfer.domain.repository;

import com.bankx.transfer.domain.model.OutboxEvent;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Порт для работы с исходящими событиями в рамках Outbox Pattern.
 * Определяет контракты для работы с бизнес-событиями Saga процесса.
 *
 * Основная задача - обеспечить надежную доставку бизнес-событий через Kafka
 * даже при временной недоступности брокера сообщений.
 *
 * Интерфейс находится в domain слое, так как события являются частью
 * бизнес-процессов Transfer Service (Saga Pattern).
 */
public interface OutboxEventRepository {

    /**
     * Сохранить событие в outbox.
     * Используется при создании новых бизнес-событий вместо прямой отправки в Kafka.
     *
     * @param event доменное событие для сохранения
     * @return сохраненное событие с присвоенным ID
     */
    OutboxEvent save(OutboxEvent event);

    /**
     * Найти событие по идентификатору.
     * Используется для обработки конкретных событий и отладки.
     *
     * @param id уникальный идентификатор события
     * @return Optional с событием, если найдено
     */
    Optional<OutboxEvent> findById(Long id);

    /**
     * Найти все необработанные события.
     * Используется OutboxEventPublisher для периодической обработки событий.
     *
     * @param maxRetries максимальное количество попыток для фильтрации
     * @return список событий, требующих обработки (NEW или FAILED с retryCount < maxRetries)
     */
    List<OutboxEvent> findUnprocessedEvents(int maxRetries);

    /**
     * Найти события по типу агрегата и идентификатору агрегата.
     * Используется для:
     * - Проверки идемпотентности (не создано ли уже событие для этого агрегата)
     * - Отладки и мониторинга
     * - Восстановления состояния Saga
     *
     * @param aggregateType тип агрегата (например, "Transfer")
     * @param aggregateId идентификатор агрегата
     * @return список событий для указанного агрегата
     */
    List<OutboxEvent> findByAggregateTypeAndAggregateId(String aggregateType, UUID aggregateId);

    /**
     * Найти события по типу события.
     * Используется для:
     * - Обработки определенных типов событий
     * - Аналитики и мониторинга
     * - Тестирования конкретных сценариев
     *
     * @param eventType тип события (например, "DEBIT_REQUEST")
     * @return список событий указанного типа
     */
    List<OutboxEvent> findByEventType(String eventType);

    /**
     * Обновить событие.
     * Используется после изменения состояния события (статус, retryCount и т.д.)
     *
     * @param event обновляемое событие
     * @return обновленное событие
     */
    OutboxEvent update(OutboxEvent event);

    /**
     * Удалить событие.
     * Используется для cleanup обработанных событий вручную.
     *
     * @param event событие для удаления
     */
    void delete(OutboxEvent event);

    /**
     * Удалить все обработанные события старше указанной даты.
     * Используется для периодической очистки базы данных от старых событий.
     *
     * @param beforeDate дата, ранее которой события считаются старыми
     */
    int deleteOldProcessedEvents(LocalDateTime beforeDate);

    /**
     * Проверить существование события по идентификатору агрегата и типу события.
     * Используется для обеспечения идемпотентности - предотвращения дублирования событий.
     *
     * @param aggregateId идентификатор агрегата
     * @param eventType тип события
     * @return true если событие уже существует
     */
    boolean existsByAggregateIdAndEventType(UUID aggregateId, String eventType);

    /**
     * Найти необработанные события с ограничением количества.
     * Используется для batch обработки с контролем размера batch.
     *
     * @param maxRetries максимальное количество попыток
     * @param limit ограничение количества возвращаемых событий
     * @return список событий (не более limit)
     */
    List<OutboxEvent> findUnprocessedEventsWithLimit(int maxRetries, int limit);

    /**
     * Находит события для обработки фоновым процессом OutboxEventPublisher.
     * Используется OutboxEventPublisher для выборки событий, требующих отправки в Kafka.
     * Критерии отбора:
     * - Статус: NEW (новые события) или FAILED (события с ошибкой отправки)
     * - retry_count < maxRetries (не превышен лимит повторных попыток)
     * - Сортировка по created_at ASC (обработка в порядке создания)
     * - Ограничение по batch_size для контроля нагрузки
     *
     * @param maxRetries максимальное количество разрешенных попыток отправки
     * @param batchSize ограничение количества возвращаемых событий за один запрос
     * @return список событий, требующих обработки
     */
    List<OutboxEvent> findEventsForProcessing(int maxRetries, int batchSize);

    /**
     * Пометить событие как обрабатываемое.
     * Оптимистичная блокировка для предотвращения конкурирующей обработки.
     *
     * @param eventId идентификатор события
     */
    void markAsProcessing(Long eventId);

    /**
     * Пометить событие как успешно отправленное.
     * Фиксирует факт успешной доставки события в Kafka.
     *
     * @param eventId идентификатор события
     */
    void markAsSent(Long eventId);

    /**
     * Пометить событие как неудачное.
     * Фиксирует ошибку отправки и увеличивает счетчик попыток.
     *
     * @param eventId идентификатор события
     * @param errorMessage сообщение об ошибке
     */
    void markAsFailed(Long eventId, String errorMessage);

    /**
     * Поиск событий по статусу.
     * Используется для мониторинга и восстановления застрявших событий.
     *
     * @param status статус события (NEW, PROCESSING, SENT, FAILED)
     * @return список событий с указанным статусом
     */
    List<OutboxEvent> findByStatus(String status);
}