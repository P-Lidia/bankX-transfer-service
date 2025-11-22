package com.bankx.transfer.application.service;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.domain.repository.OutboxEventRepository;
import com.bankx.transfer.shared.util.JsonConverter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

/**
 * Сервис для работы с Outbox событиями в рамках Outbox Pattern.
 *
 * Основная задача - обеспечить надежное сохранение бизнес-событий в рамках
 * распределенных транзакций согласно требованиям ТЗ по надежности (5.1).
 *
 * Отвечает за:
 * - Создание и сохранение событий в outbox таблицу
 * - Гарантию сохранения событий даже при временной недоступности Kafka
 * - Поддержку correlationId для трассировки распределенных транзакций
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonConverter jsonConverter;

    /**
     * Создание нового outbox события с поддержкой correlationId.
     * Используется при порождении бизнес-событий в Saga процессе.
     *
     * @param aggregateType тип агрегата (например, "Transfer")
     * @param aggregateId идентификатор агрегата (ID перевода)
     * @param eventType тип события согласно ТЗ (DEBIT_REQUEST, CREDIT_REQUEST и т.д.)
     * @param payload полезная нагрузка события в виде Map
     * @param correlationId идентификатор корреляции для трассировки согласно требованию 2.1 ТЗ
     *
     * @throws RuntimeException если не удалось сохранить событие
     */
    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload,
                                  UUID correlationId) { // Изменено с String на UUID
        try {
            // Сериализация payload в JSON строку для хранения в JSONB колонке
            String payloadJson = jsonConverter.toJson(payload);

            // Создание доменного объекта OutboxEvent с использованием фабричного метода
            OutboxEvent outboxEvent = OutboxEvent.create(
                    aggregateType,
                    aggregateId,
                    eventType,
                    payloadJson,
                    correlationId // Сохраняем correlationId для трассировки
            );

            // Сохранение события в репозиторий (в рамках текущей транзакции)
            outboxEventRepository.save(outboxEvent);

            log.debug("Outbox event created successfully: type={}, aggregate={}, correlation={}",
                    eventType, aggregateId, correlationId);

        } catch (Exception e) {
            log.error("Failed to create outbox event: type={}, aggregate={}, correlation={}",
                    eventType, aggregateId, correlationId, e);
            throw new RuntimeException("Outbox event creation failed", e);
        }
    }

    /**
     * Перегруженный метод для создания outbox события без явного указания correlationId.
     * Используется для обратной совместимости и сценариев, где correlationId не требуется.
     * Автоматически генерирует correlationId на основе UUID.
     *
     * @param aggregateType тип агрегата
     * @param aggregateId идентификатор агрегата
     * @param eventType тип события
     * @param payload полезная нагрузка события
     */
    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload) {
        // Генерация correlationId на основе UUID для обеспечения уникальности
        UUID generatedCorrelationId = UUID.randomUUID(); // Изменено на UUID
        createOutboxEvent(aggregateType, aggregateId, eventType, payload, generatedCorrelationId);
    }
}