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
 * Сервис для работы с исходящими событиями в рамках Outbox Pattern.
 * Отвечает за:
 * - Создание и сохранение бизнес-событий в outbox таблицу
 * - Гарантию сохранения событий даже при недоступности Kafka
 * - Поддержку correlationId для трассировки распределенных транзакций
 * - Сериализацию payload в JSON для хранения в колонке JSONB
 *
 * Реализует требования ТЗ по надежной доставке событий (5.1) и
 * обеспечивает атомарность бизнес-операций и публикации событий.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonConverter jsonConverter;

    /**
     * Создает и сохраняет outbox событие с гарантией наличия correlationId.
     * Используется для порождения бизнес-событий в Saga процессе переводов.
     *
     * @param aggregateType тип агрегата-источника события (например, "Transfer")
     * @param aggregateId идентификатор агрегата (ID перевода)
     * @param eventType тип бизнес-события (DEBIT_REQUEST, CREDIT_REQUEST, etc.)
     * @param payload полезная нагрузка события в виде Map
     * @param correlationId идентификатор корреляции для трассировки
     *
     * @throws RuntimeException если не удалось сохранить событие
     *
     * @implNote Метод транзакционный - сохраняет событие в той же транзакции,
     * что и бизнес-данные. Гарантирует, что событие либо сохранится вместе
     * с бизнес-данными, либо вся операция откатится.
     */
    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload,
                                  UUID correlationId) {
        try {
            log.info("Creating outbox event: type={}, aggregateId={}, correlationId={}",
                    eventType, aggregateId, correlationId);

            // Гарантируем наличие correlationId для трассировки
            // Защита от null значений в обязательном поле БД
            if (correlationId == null) {
                log.warn("CorrelationId is null, generating new one");
                correlationId = UUID.randomUUID();
            }

            // Сериализуем payload в JSON для хранения в колонке JSONB
            // JsonConverter обеспечивает единообразную сериализацию
            String payloadJson = jsonConverter.toJson(payload);
            log.debug("Payload JSON: {}", payloadJson);

            // Создаем доменный объект outbox события
            // Builder pattern обеспечивает удобное создание сложных объектов
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .payload(payloadJson)
                    .correlationId(correlationId)
                    .build();

            // Сохраняем событие в outbox таблицу
            // Событие сохраняется со статусом NEW для последующей обработки
            OutboxEvent savedEvent = outboxEventRepository.save(outboxEvent);
            log.info("Outbox event saved successfully: id={}, correlationId={}",
                    savedEvent.getId(), savedEvent.getCorrelationId());

        } catch (Exception e) {
            log.error("Failed to create outbox event: type={}, aggregateId={}, correlationId={}",
                    eventType, aggregateId, correlationId, e);
            throw new RuntimeException("Failed to create outbox event", e);
        }
    }

    /**
     * Перегруженная версия метода для создания outbox события без явного correlationId.
     * Автоматически генерирует correlationId для случаев, когда трассировка
     * не требуется или correlationId неизвестен.
     *
     * @param aggregateType тип агрегата
     * @param aggregateId идентификатор агрегата
     * @param eventType тип события
     * @param payload полезная нагрузка
     *
     * @implNote Используется для внутренних событий или тестовых сценариев,
     * где correlationId не предоставлен внешней системой.
     */
    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload) {
        // Всегда генерируем correlationId если не предоставлен
        // Обеспечивает целостность данных и возможность трассировки
        UUID generatedCorrelationId = UUID.randomUUID();
        createOutboxEvent(aggregateType, aggregateId, eventType, payload, generatedCorrelationId);
    }
}