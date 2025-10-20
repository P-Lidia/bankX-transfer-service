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
 * РЕАЛИЗАЦИЯ РЕПОЗИТОРИЯ ДЛЯ OUTBOX СОБЫТИЙ
 *
 * НАЗНАЧЕНИЕ В АРХИТЕКТУРЕ:
 * - Ядро реализации Outbox Pattern для надежной доставки событий
 * - Гарантированное сохранение событий даже при недоступности Kafka
 * - Координация распределенных транзакций в Saga паттерне
 *
 * КАК РАБОТАЕТ OUTBOX PATTERN:
 * 1. Бизнес-логика создает событие + сохраняет в outbox в ОДНОЙ транзакции
 * 2. Отдельный процесс (OutboxEventPublisher) читает из outbox и отправляет в Kafka
 * 3. При успешной отправке помечает событие как SENT
 * 4. При ошибках - retry логика с ограничением попыток
 *
 * ПРЕИМУЩЕСТВА:
 * - Гарантированная доставка (at-least-once семантика)
 * - Отказоустойчивость к сбоям Kafka
 * - Сохранение порядка событий
 * - Трассировка через correlationId
 */
@Repository
public class JpaOutboxEventRepository implements OutboxEventRepository {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxEventRepository.class);

    // Spring Data репозиторий для JPA операций
    private final SpringDataOutboxEventRepository springDataOutboxEventRepository;
    // Маппер для преобразования Domain <-> JPA entity
    private final OutboxEventMapper outboxEventMapper;

    public JpaOutboxEventRepository(SpringDataOutboxEventRepository springDataOutboxEventRepository,
                                    OutboxEventMapper outboxEventMapper) {
        this.springDataOutboxEventRepository = springDataOutboxEventRepository;
        this.outboxEventMapper = outboxEventMapper;
    }

    /**
     * СОХРАНЕНИЕ НОВОГО OUTBOX СОБЫТИЯ
     *
     * ИСПОЛЬЗУЕТСЯ КОГДА:
     * - TransferService создает новый перевод → DEBIT_REQUEST
     * - Получен DEBIT_CONFIRMED → CREDIT_REQUEST
     * - Получен CREDIT_FAILED → COMPENSATE_DEBIT
     * - Завершен перевод → TRANSFER_COMPLETED/FAILED
     *
     * КРИТИЧЕСКАЯ ВАЖНОСТЬ:
     * - Сохранение события и бизнес-данных в ОДНОЙ транзакции
     * - Гарантия, что событие не потеряется даже при падении сервиса
     *
     * @param event доменная модель события (без ID)
     * @return сохраненное событие с присвоенным ID
     */
    @Override
    @Transactional
    public OutboxEvent save(OutboxEvent event) {
        log.debug("Сохранение outbox события: aggregateType={}, eventType={}",
                event.getAggregateType(), event.getEventType());

        try {
            // Domain -> JPA entity преобразование
            OutboxEventEntity entity = outboxEventMapper.toEntity(event);
            // INSERT в таблицу outbox_events
            OutboxEventEntity savedEntity = springDataOutboxEventRepository.save(entity);
            // JPA entity -> Domain обратное преобразование
            OutboxEvent savedEvent = outboxEventMapper.toDomain(savedEntity);

            log.info("Outbox событие успешно сохранено: id={}, aggregateType={}, eventType={}",
                    savedEvent.getId(), savedEvent.getAggregateType(), savedEvent.getEventType());
            return savedEvent;

        } catch (Exception e) {
            log.error("Ошибка при сохранении outbox события: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось сохранить outbox событие", e);
        }
    }

    /**
     * ПОИСК НЕОБРАБОТАННЫХ СОБЫТИЙ ДЛЯ OUTBOXEventPublisher
     *
     * КРИТЕРИИ ОТБОРА:
     * - Статус: NEW (новые) или FAILED (неудачные с ретраями)
     * - retryCount < maxRetries (не превышен лимит попыток)
     * - Сортировка по created_at ASC (старые вперед)
     *
     * ИСПОЛЬЗУЕТСЯ В:
     * - OutboxEventPublisher для фоновой отправки в Kafka
     * - Административных интерфейсах для мониторинга
     *
     * @param maxRetries максимальное количество разрешенных попыток отправки
     * @return список событий, требующих обработки
     */
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

    /**
     * ПОИСК СОБЫТИЙ ДЛЯ КОНКРЕТНОГО АГРЕГАТА (TRANSFER)
     *
     * ИСПОЛЬЗУЕТСЯ ДЛЯ:
     * - Восстановления состояния Saga после сбоя
     * - Отладки и анализа цепочки событий перевода
     * - Проверки идемпотентности (не создано ли дублирующих событий)
     *
     * @param aggregateType тип агрегата (всегда "Transfer" в нашем случае)
     * @param aggregateId ID перевода
     * @return все события связанные с этим переводом
     */
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

    /**
     * ПАКЕТНАЯ ОБРАБОТКА С ОГРАНИЧЕНИЕМ (LIMIT)
     *
     * ОПТИМИЗАЦИЯ ДЛЯ:
     * - Предотвращение memory overflow при большом количестве событий
     * - Распределение нагрузки на Kafka брокер
     * - Контроль размера транзакций в OutboxEventPublisher
     *
     * @param maxRetries максимальное количество попыток
     * @param limit ограничение количества возвращаемых событий
     * @return не более limit событий для обработки
     */
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

    /**
     * ИЗМЕНЕНИЕ СТАТУСА СОБЫТИЯ НА "В ПРОЦЕССЕ ОТПРАВКИ"
     *
     * ОПТИМИСТИЧЕСКАЯ БЛОКИРОВКА:
     * - Предотвращает конкурирующую обработку одного события
     * - OutboxEventPublisher помечает событие как PROCESSING перед отправкой
     * - Если несколько инстансов сервиса - только один сможет обработать
     *
     * @param eventId ID события для блокировки
     */
    @Override
    @Transactional
    public void markAsProcessing(Long eventId) {
        log.debug("Пометка outbox события как обрабатываемого: id={}", eventId);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsProcessing(); // Устанавливает статус PROCESSING
                springDataOutboxEventRepository.save(event); // UPDATE запрос
                log.debug("Outbox событие помечено как PROCESSING: id={}", eventId);
            } else {
                log.warn("Outbox событие с ID {} не найдено для пометки как PROCESSING", eventId);
            }
        } catch (Exception e) {
            log.error("Ошибка при пометке outbox события как обрабатываемого: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось пометить outbox событие как обрабатываемое", e);
        }
    }

    /**
     * ОТМЕТКА УСПЕШНОЙ ОТПРАВКИ В KAFKA
     *
     * ВЫЗЫВАЕТСЯ КОГДА:
     * - KafkaTemplate подтверждает доставку сообщения
     * - SendResult не содержит исключений
     *
     * УСТАНАВЛИВАЕТ:
     * - Статус: SENT (отправлено)
     * - processed_at: текущее время
     * - error_message: null (очищает предыдущие ошибки)
     *
     * @param eventId ID успешно отправленного события
     */
    @Override
    @Transactional
    public void markAsSent(Long eventId) {
        log.debug("Пометка outbox события как отправленного: id={}", eventId);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsSent(LocalDateTime.now()); // Статус SENT + время обработки
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

    /**
     * ОБРАБОТКА ОШИБКИ ОТПРАВКИ В KAFKA
     *
     * ВЫЗЫВАЕТСЯ КОГДА:
     * - KafkaTemplate возвращает исключение
     * - Истекает timeout отправки
     * - Брокер Kafka недоступен
     *
     * ДЕЙСТВИЯ:
     * - Статус: FAILED (неудачно)
     * - retry_count: увеличивается на 1
     * - error_message: сохраняется причина ошибки
     *
     * @param eventId ID события с ошибкой отправки
     * @param errorMessage описание ошибки для диагностики
     */
    @Override
    @Transactional
    public void markAsFailed(Long eventId, String errorMessage) {
        log.debug("Пометка outbox события как неудачного: id={}, error={}", eventId, errorMessage);

        try {
            Optional<OutboxEventEntity> eventOpt = springDataOutboxEventRepository.findById(eventId);
            if (eventOpt.isPresent()) {
                OutboxEventEntity event = eventOpt.get();
                event.markAsFailed(errorMessage); // Статус FAILED + инкремент retry_count
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

    /**
     * СТАТИСТИКА ДЛЯ МОНИТОРИНГА И АЛЕРТИНГА
     *
     * ИСПОЛЬЗУЕТСЯ В:
     * - Prometheus метриках для Grafana дашбордов
     * - Health checks и readiness probes
     * - Алертинге при накоплении FAILED событий
     *
     * @return агрегированная статистика по всем событиям
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
     * DTO ДЛЯ СТАТИСТИКИ OUTBOX СОБЫТИЙ
     *
     * СОДЕРЖИТ АГРЕГИРОВАННЫЕ ДАННЫЕ:
     * - totalEvents: общее количество событий в outbox
     * - newEvents: новые события, ожидающие отправки
     * - failedEvents: неудачные отправки (требуют ретраев)
     * - sentEvents: успешно отправленные события
     * - processingEvents: события в процессе отправки
     *
     * ИСПОЛЬЗУЕТСЯ ДЛЯ:
     * - Визуализации в административных панелях
     * - Принятия решений о масштабировании
     * - Выявления проблем в доставке событий
     */
    public static class OutboxEventStatistics {
        private final long totalEvents;
        private final long newEvents;
        private final long failedEvents;
        private final long sentEvents;
        private final long processingEvents;

        public OutboxEventStatistics(long totalEvents, long newEvents, long failedEvents,
                                     long sentEvents, long processingEvents) {
            this.totalEvents = totalEvents;
            this.newEvents = newEvents;
            this.failedEvents = failedEvents;
            this.sentEvents = sentEvents;
            this.processingEvents = processingEvents;
        }

        // Геттеры для доступа к данным
        public long getTotalEvents() { return totalEvents; }
        public long getNewEvents() { return newEvents; }
        public long getFailedEvents() { return failedEvents; }
        public long getSentEvents() { return sentEvents; }
        public long getProcessingEvents() { return processingEvents; }

        @Override
        public String toString() {
            return String.format(
                    "OutboxEventStatistics{total=%d, new=%d, failed=%d, sent=%d, processing=%d}",
                    totalEvents, newEvents, failedEvents, sentEvents, processingEvents
            );
        }
    }
}