package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.repository.ProcessedEventRepository;
import com.bankx.transfer.infrastructure.persistence.entity.ProcessedEventEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * РЕАЛИЗАЦИЯ РЕПОЗИТОРИЯ ДЛЯ ОБРАБОТАННЫХ СОБЫТИЙ (ИДЕМПОТЕНТНОСТЬ)
 *
 * НАЗНАЧЕНИЕ:
 * - Предотвращение повторной обработки дублирующих событий Kafka
 * - Обеспечение идемпотентности согласно требованию 2.1 ТЗ
 * - Отслеживание уже обработанных event_id для исключения дубликатов
 *
 * КАК РАБОТАЕТ:
 * 1. При получении события из Kafka проверяем existsByEventId()
 * 2. Если событие уже обработано - пропускаем обработку
 * 3. После успешной обработки сохраняем eventId через save()
 * 4. Периодически очищаем старые записи через deleteOldProcessedEvents()
 *
 * АРХИТЕКТУРА:
 * - Реализует доменный порт ProcessedEventRepository
 * - Использует Spring Data JPA для работы с БД
 * - Работает с таблицей processed_events (event_id UNIQUE)
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class JpaProcessedEventRepository implements ProcessedEventRepository {

    // Spring Data репозиторий - автоматически генерирует SQL запросы
    private final SpringDataProcessedEventRepository springDataRepository;

    /**
     * ПРОВЕРКА - БЫЛО ЛИ СОБЫТИЕ УЖЕ ОБРАБОТАНО
     *
     * ИСПОЛЬЗУЕТСЯ В:
     * - Kafka Consumer'ах перед обработкой входящих событий
     * - TransferService для проверки дублирующих команд
     *
     * ПРИНЦИП РАБОТЫ:
     * - Ищет запись в таблице processed_events по event_id
     * - Если запись найдена - событие уже обработано (идемпотентность)
     * - Если не найдено - событие новое, можно обрабатывать
     *
     * @param eventId уникальный идентификатор события (например, из Kafka message)
     * @return true если событие уже обработано (дубликат), false если новое
     */
    @Override
    @Transactional(readOnly = true) // Только чтение, не блокирует транзакцию
    public boolean existsByEventId(String eventId) {
        log.debug("Checking if event already processed: {}", eventId);
        boolean exists = springDataRepository.existsByEventId(eventId);
        log.debug("Event {} processed status: {}", eventId, exists);
        return exists;
    }

    /**
     * СОХРАНЕНИЕ ИНФОРМАЦИИ ОБ ОБРАБОТАННОМ СОБЫТИИ
     *
     * ИСПОЛЬЗУЕТСЯ ПОСЛЕ:
     * - Успешной обработки события Kafka Consumer'ом
     * - Создания нового перевода в TransferService
     * - Любой операции, которая должна быть идемпотентной
     *
     * ПРИНЦИП РАБОТЫ:
     * 1. Создает новый ProcessedEventEntity с eventId
     * 2. Spring Data автоматически устанавливает createdAt (текущее время)
     * 3. Сохраняет в таблицу processed_events
     * 4. При попытке сохранить дубликат - исключение (UNIQUE constraint)
     *
     * @param eventId уникальный идентификатор обработанного события
     */
    @Override
    @Transactional // Транзакция гарантирует атомарность сохранения
    public void save(String eventId) {
        log.debug("Saving processed event: {}", eventId);

        try {
            // Создаем JPA сущность через Builder pattern
            ProcessedEventEntity entity = ProcessedEventEntity.builder()
                    .eventId(eventId)
                    .build();

            // Spring Data автоматически генерирует INSERT запрос
            springDataRepository.save(entity);
            log.debug("Successfully saved processed event: {}", eventId);

        } catch (Exception e) {
            log.error("Failed to save processed event: {}", eventId, e);
            throw new RuntimeException("Failed to save processed event", e);
        }
    }

    /**
     * ОЧИСТКА СТАРЫХ ОБРАБОТАННЫХ СОБЫТИЙ
     *
     * НЕОБХОДИМОСТЬ:
     * - Предотвращение неограниченного роста таблицы processed_events
     * - Улучшение производительности запросов
     * - Соответствие политикам хранения данных
     *
     * КОГДА ВЫЗЫВАЕТСЯ:
     * - По расписанию (например, каждые 30 дней)
     * - Через ProcessedEventsCleanupScheduler
     * - Вручную через административный интерфейс
     *
     * @param beforeDate удаляет события, созданные раньше этой даты
     */
    @Override
    @Transactional
    public void deleteOldProcessedEvents(LocalDateTime beforeDate) {
        log.debug("Deleting processed events older than: {}", beforeDate);

        try {
            // Выполняет DELETE запрос с условием по дате
            int deletedCount = springDataRepository.deleteByCreatedAtBefore(beforeDate);
            log.info("Deleted {} old processed events", deletedCount);

        } catch (Exception e) {
            log.error("Failed to delete old processed events", e);
            throw new RuntimeException("Failed to delete old processed events", e);
        }
    }
}