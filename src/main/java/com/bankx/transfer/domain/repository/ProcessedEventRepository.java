package com.bankx.transfer.domain.repository;

import com.bankx.transfer.domain.model.ProcessedEvent;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Доменный порт для работы с журналом обработанных событий (processed_events).
 * Определяет контракты для идемпотентности потребления сообщений и аудита обработки.
 * Отвечает за:
 * - Абстракцию доступа к данным processed_events
 * - Сохранение/проверку уникальности событий
 * - Поиск и выгрузку событий по переводу
 * - Очистку устаревших записей
 */
public interface ProcessedEventRepository {

    /**
     * Сохранение события обработки.
     * Гарантирует фиксацию факта обработки конкретного сообщения.
     *
     * @param event доменная модель обработанного события
     * @return сохранённое событие
     */
    ProcessedEvent save(ProcessedEvent event);

    /**
     * Поиск события по идентификатору записи.
     *
     * @param id первичный ключ записи
     * @return Optional с событием, если найдено
     */
    Optional<ProcessedEvent> findById(Long id);

    /**
     * Поиск события по глобальному идентификатору сообщения (идемпотентность).
     *
     * @param eventId уникальный идентификатор входящего сообщения
     * @return Optional с событием, если найдено
     */
    Optional<ProcessedEvent> findByEventId(String eventId);

    /**
     * Проверка, существует ли событие с данным eventId.
     * Используется для защиты от повторной обработки дублей сообщений.
     *
     * @param eventId уникальный идентификатор входящего сообщения
     * @return true, если событие уже зафиксировано
     */
    boolean existsByEventId(String eventId);

    /**
     * Получить последние события по переводу (например, для аудита/отладки).
     *
     * @param transferId идентификатор перевода
     * @param limit      максимальное количество записей
     * @return список последних событий по времени создания
     */
    List<ProcessedEvent> findRecentByTransferId(UUID transferId, int limit);

    /**
     * Получить все события по переводу в хронологическом порядке.
     *
     * @param transferId идентификатор перевода
     * @return список событий от более ранних к более поздним
     */
    List<ProcessedEvent> findAllByTransferIdOrderByCreatedAtAsc(UUID transferId);

    /**
     * Удалить устаревшие записи (например, ротация лога).
     *
     * @param before удалить все записи, созданные ранее указанной даты/времени
     */
    void deleteOlderThan(LocalDateTime before);
}