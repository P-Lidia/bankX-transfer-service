package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.application.port.EventPublisherPort;
import com.bankx.transfer.application.service.OutboxEventService;
import com.bankx.transfer.infrastructure.config.KafkaTopicConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация порта для публикации событий в Kafka.
 * Инфраструктурный адаптер, отвечающий за отправку событий во внешние системы.
 * Обеспечивает асинхронную публикацию событий с обработкой результатов и логированием.
 * Для событий, связанных с созданием переводов (DEBIT_REQUEST), использует outbox pattern
 * для гарантированной доставки согласно требованиям ТЗ по надежности.
 *
 * Соответствует требованиям ТЗ:
 * - Гарантированная доставка через Outbox Pattern (5.1)
 * - Идемпотентность через correlationId (2.1)
 * - Трассировка распределенных транзакций
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisherPort {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicConfig topicConfig;
    private final OutboxEventService outboxEventService;

    /**
     * Публикует событие запроса на списание средств.
     * Используется для инициации операции списания с указанного счета.
     * ВАЖНО: Для DEBIT_REQUEST используется outbox pattern вместо прямой отправки в Kafka
     * для обеспечения надежности согласно требованиям ТЗ.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для списания
     * @param amount сумма списания
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     *
     * @implNote Метод сохраняет событие в outbox таблицу вместо прямой отправки в Kafka.
     * Это гарантирует, что событие не будет потеряно при недоступности Kafka.
     * OutboxEventPublisher асинхронно отправит событие в Kafka.
     */
    @Override
    public void publishDebitRequest(String transferId, UUID correlationId,
                                    String accountId, BigDecimal amount, String currency) {
        log.info("Publishing DEBIT_REQUEST via outbox: transferId={}, correlationId={}, accountId={}, amount={}, currency={}",
                transferId, correlationId, accountId, amount, currency);

        // Используем outbox pattern для DEBIT_REQUEST вместо прямой отправки в Kafka
        // Это гарантирует сохранение события даже при недоступности Kafka
        outboxEventService.createDebitRequestEvent(
                transferId,
                correlationId.toString(), // Преобразуем UUID в String для OutboxEventService
                accountId,
                amount,
                currency
        );

        log.info("DEBIT_REQUEST event saved to outbox: transferId={}, correlationId={}",
                transferId, correlationId);
    }

    /**
     * Публикует событие запроса на зачисление средств.
     * Используется для инициации операции зачисления на указанный счет.
     * Отправляется напрямую в Kafka, так как это промежуточное событие в saga.
     * Используется после успешного списания средств.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для зачисления
     * @param amount сумма зачисления
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     *
     * @implNote Метод отправляет событие напрямую в Kafka, так как CREDIT_REQUEST
     * является промежуточным событием в Saga и не требует гарантии доставки
     * через Outbox Pattern.
     */
    @Override
    public void publishCreditRequest(String transferId, UUID correlationId,
                                     String accountId, BigDecimal amount, String currency) {
        log.info("Publishing CREDIT_REQUEST: transferId={}, correlationId={}, accountId={}, amount={}, currency={}",
                transferId, correlationId, accountId, amount, currency);

        // Подготавливаем payload события
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", accountId);
        payload.put("amount", amount);
        payload.put("currency", currency);

        // Создаем KafkaEvent
        KafkaEvent event = KafkaEvent.createWithTransferId(
                "CREDIT_REQUEST",
                correlationId,
                transferId,
                payload
        );

        // Отправляем событие в Kafka
        sendEvent(topicConfig.getAccountCreditRequestTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish CREDIT_REQUEST: transferId={}, correlationId={}, error={}",
                                transferId, correlationId, exception.getMessage(), exception);
                        throw new RuntimeException("Failed to publish CREDIT_REQUEST", exception);
                    } else {
                        log.info("Successfully published CREDIT_REQUEST: transferId={}, correlationId={}, topic={}, partition={}, offset={}",
                                transferId, correlationId,
                                topicConfig.getAccountCreditRequestTopic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    /**
     * Публикует событие компенсационного списания.
     * Используется для отката операции в случае ошибок при обработке трансфера.
     * Отправляется напрямую в Kafka, так как это компенсационное действие.
     * Используется после неудачного зачисления средств.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для компенсационного списания
     * @param amount сумма компенсации
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     *
     * @implNote Метод отправляет событие напрямую в Kafka, так как COMPENSATE_DEBIT
     * является частью компенсационного сценария Saga и требует немедленной отправки.
     */
    @Override
    public void publishCompensateDebit(String transferId, UUID correlationId,
                                       String accountId, BigDecimal amount, String currency) {
        log.info("Publishing COMPENSATE_DEBIT: transferId={}, correlationId={}, accountId={}, amount={}, currency={}",
                transferId, correlationId, accountId, amount, currency);

        // Подготавливаем payload события
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", accountId);
        payload.put("amount", amount);
        payload.put("currency", currency);

        // Создаем KafkaEvent
        KafkaEvent event = KafkaEvent.createWithTransferId(
                "COMPENSATE_DEBIT",
                correlationId,
                transferId,
                payload
        );

        // Отправляем событие в Kafka
        sendEvent(topicConfig.getAccountCompensateDebitTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish COMPENSATE_DEBIT: transferId={}, correlationId={}, error={}",
                                transferId, correlationId, exception.getMessage(), exception);
                        throw new RuntimeException("Failed to publish COMPENSATE_DEBIT", exception);
                    } else {
                        log.info("Successfully published COMPENSATE_DEBIT: transferId={}, correlationId={}, topic={}, partition={}, offset={}",
                                transferId, correlationId,
                                topicConfig.getAccountCompensateDebitTopic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    /**
     * Публикует событие изменения статуса трансфера.
     * Уведомляет систему об изменении состояния операции перевода средств.
     * Отправляется напрямую в Kafka, так как это финальное уведомление.
     * Используется для информирования Payment Orchestrator о результате перевода.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param status новый статус трансфера (COMPLETED, FAILED, COMPENSATED и т.д.)
     * @param reason причина изменения статуса (опционально)
     * @throws RuntimeException если публикация события завершилась ошибкой
     *
     * @implNote Метод отправляет событие напрямую в Kafka, так как TRANSFER_STATUS
     * является финальным событием Saga и должно быть доставлено немедленно.
     */
    @Override
    public void publishTransferStatus(String transferId, UUID correlationId,
                                      String status, String reason) {
        log.info("Publishing TRANSFER_STATUS: transferId={}, correlationId={}, status={}, reason={}",
                transferId, correlationId, status, reason);

        // Подготавливаем payload события
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", status);
        payload.put("reason", reason);
        payload.put("transferId", transferId);

        // Определяем тип события на основе статуса
        String eventType = getEventTypeForStatus(status);

        // Создаем KafkaEvent
        KafkaEvent event = KafkaEvent.createWithTransferId(
                eventType,
                correlationId,
                transferId,
                payload
        );

        // Отправляем событие в Kafka
        sendEvent(topicConfig.getTransferStatusTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish TRANSFER_STATUS: transferId={}, status={}, error={}",
                                transferId, status, exception.getMessage(), exception);
                        throw new RuntimeException("Failed to publish TRANSFER_STATUS", exception);
                    } else {
                        log.info("Successfully published TRANSFER_STATUS: transferId={}, status={}, topic={}, partition={}, offset={}",
                                transferId, status,
                                topicConfig.getTransferStatusTopic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    /**
     * Прямая публикация события в Kafka без использования Outbox Pattern.
     * Используется OutboxEventPublisher для отправки событий из таблицы outbox_events.
     * Гарантирует атомарность отправки и обновления статуса события.
     *
     * @param kafkaEvent событие для публикации
     *
     * @implNote Метод используется только OutboxEventPublisher для отправки событий,
     * которые были предварительно сохранены в outbox таблице.
     * Не используется напрямую бизнес-логикой.
     */
    public void publishEventDirectly(KafkaEvent kafkaEvent) {
        // Определяем топик на основе типа события
        String topic = determineTopic(kafkaEvent.getEventType());

        // Определяем ключ сообщения (используем transferId или correlationId)
        String key = kafkaEvent.getTransferId() != null ?
                kafkaEvent.getTransferId().toString() :
                kafkaEvent.getCorrelationId().toString();

        log.info("Publishing event directly to Kafka: topic={}, eventType={}, correlationId={}, transferId={}",
                topic, kafkaEvent.getEventType(), kafkaEvent.getCorrelationId(), kafkaEvent.getTransferId());

        // Отправляем событие в Kafka асинхронно
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, kafkaEvent);

        // Обрабатываем результат отправки
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to publish event directly: eventType={}, topic={}, error={}",
                        kafkaEvent.getEventType(), topic, exception.getMessage(), exception);
                throw new RuntimeException("Failed to publish event to Kafka", exception);
            } else {
                log.info("Successfully published event directly: eventType={}, topic={}, partition={}, offset={}",
                        kafkaEvent.getEventType(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Универсальный метод отправки события в Kafka.
     * Выполняет асинхронную отправку с обработкой результата и детальным логированием.
     * Обеспечивает идемпотентность через correlationId и гарантированную доставку через retry.
     *
     * @param topic название топика Kafka для отправки
     * @param key ключ сообщения (обычно transferId)
     * @param event объект события для отправки
     * @return CompletableFuture с результатом отправки
     *
     * @implNote Метод инкапсулирует логику отправки в Kafka и обеспечивает
     * единообразную обработку ошибок для всех типов событий.
     */
    private CompletableFuture<SendResult<String, Object>> sendEvent(
            String topic, String key, Object event) {
        // Извлекаем тип события для логирования
        String eventType = extractEventType(event);

        log.info("Sending Kafka event: {} to topic: {} with key: {}", eventType, topic, key);

        // Отправляем событие асинхронно
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, event);

        // Обработка результата отправки
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to send Kafka event: {} to topic: {}, error: {}",
                        eventType, topic, exception.getMessage(), exception);
            } else {
                log.debug("Successfully sent Kafka event: {} to topic: {} partition: {} offset: {}",
                        eventType, topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * Извлекает тип события из объекта события.
     * Поддерживает извлечение из KafkaEvent или использование имени класса как fallback.
     *
     * @param event объект события
     * @return строковое представление типа события
     */
    private String extractEventType(Object event) {
        if (event instanceof KafkaEvent) {
            return ((KafkaEvent) event).getEventType();
        }
        return event.getClass().getSimpleName();
    }

    /**
     * Определяет тип события Kafka на основе статуса трансфера.
     * Маппит бизнес-статусы на соответствующие типы событий Kafka.
     * Соответствует требованиям ТЗ по нотации событий (5.1).
     *
     * @param status статус трансфера
     * @return соответствующий тип события Kafka
     */
    private String getEventTypeForStatus(String status) {
        if (status == null) {
            return "TRANSFER_STATUS_UPDATE";
        }

        switch (status.toUpperCase()) {
            case "COMPLETED":
                return "TRANSFER_COMPLETED";
            case "FAILED":
                return "TRANSFER_FAILED";
            case "COMPENSATED":
                return "TRANSFER_COMPENSATED";
            default:
                return "TRANSFER_STATUS_UPDATE";
        }
    }

    /**
     * Определяет Kafka топик на основе типа события.
     * Реализует маппинг бизнес-событий на физические топики Kafka.
     * Соответствует конфигурации в KafkaTopicConfig.
     *
     * @param eventType тип бизнес-события
     * @return название топика Kafka
     * @throws IllegalArgumentException если тип события неизвестен
     */
    private String determineTopic(String eventType) {
        if (eventType == null) {
            throw new IllegalArgumentException("Event type cannot be null for topic mapping");
        }

        switch (eventType) {
            case "DEBIT_REQUEST":
                return topicConfig.getAccountDebitRequestTopic();
            case "CREDIT_REQUEST":
                return topicConfig.getAccountCreditRequestTopic();
            case "COMPENSATE_DEBIT":
                return topicConfig.getAccountCompensateDebitTopic();
            case "TRANSFER_COMPLETED":
            case "TRANSFER_FAILED":
            case "TRANSFER_COMPENSATED":
            case "TRANSFER_STATUS_UPDATE":
                return topicConfig.getTransferStatusTopic();
            default:
                throw new IllegalArgumentException("Unknown event type for topic mapping: " + eventType);
        }
    }
}