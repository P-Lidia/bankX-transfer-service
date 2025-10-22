package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.application.port.EventPublisherPort;
import com.bankx.transfer.infrastructure.config.KafkaTopicConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Реализация порта для публикации событий в Kafka.
 * Инфраструктурная реализация, отвечающая за отправку событий во внешние системы.
 * Обеспечивает асинхронную публикацию событий с обработкой результатов и логированием.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisherPort {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicConfig topicConfig;

    /**
     * Публикует событие запроса на списание средств.
     * Используется для инициации операции списания с указанного счета.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для списания
     * @param amount сумма списания
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     */
    @Override
    public void publishDebitRequest(String transferId, String correlationId,
                                    String accountId, BigDecimal amount, String currency) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", accountId);
        payload.put("amount", amount);
        payload.put("currency", currency);
        KafkaEvent event = KafkaEvent.createWithTransferId(
                "DEBIT_REQUEST", correlationId, transferId, payload);
        sendEvent(topicConfig.getAccountDebitRequestTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish DEBIT_REQUEST: transferId={}, correlationId={}",
                                transferId, correlationId, exception);
                        throw new RuntimeException("Failed to publish DEBIT_REQUEST", exception);
                    } else {
                        log.info("Successfully published DEBIT_REQUEST: transferId={}, correlationId={}",
                                transferId, correlationId);
                    }
                });
    }

    /**
     * Публикует событие запроса на зачисление средств.
     * Используется для инициации операции зачисления на указанный счет.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для зачисления
     * @param amount сумма зачисления
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     */
    @Override
    public void publishCreditRequest(String transferId, String correlationId,
                                     String accountId, BigDecimal amount, String currency) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", accountId);
        payload.put("amount", amount);
        payload.put("currency", currency);
        KafkaEvent event = KafkaEvent.createWithTransferId(
                "CREDIT_REQUEST", correlationId, transferId, payload);
        sendEvent(topicConfig.getAccountCreditRequestTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish CREDIT_REQUEST: transferId={}, correlationId={}",
                                transferId, correlationId, exception);
                        throw new RuntimeException("Failed to publish CREDIT_REQUEST", exception);
                    } else {
                        log.info("Successfully published CREDIT_REQUEST: transferId={}, correlationId={}",
                                transferId, correlationId);
                    }
                });
    }

    /**
     * Публикует событие компенсационного списания.
     * Используется для отката операции в случае ошибок при обработке трансфера.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param accountId идентификатор счета для компенсационного списания
     * @param amount сумма компенсации
     * @param currency валюта операции
     * @throws RuntimeException если публикация события завершилась ошибкой
     */
    @Override
    public void publishCompensateDebit(String transferId, String correlationId,
                                       String accountId, BigDecimal amount, String currency) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", accountId);
        payload.put("amount", amount);
        payload.put("currency", currency);
        KafkaEvent event = KafkaEvent.createWithTransferId(
                "COMPENSATE_DEBIT", correlationId, transferId, payload);
        sendEvent(topicConfig.getAccountCompensateDebitTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish COMPENSATE_DEBIT: transferId={}, correlationId={}",
                                transferId, correlationId, exception);
                        throw new RuntimeException("Failed to publish COMPENSATE_DEBIT", exception);
                    } else {
                        log.info("Successfully published COMPENSATE_DEBIT: transferId={}, correlationId={}",
                                transferId, correlationId);
                    }
                });
    }

    /**
     * Публикует событие изменения статуса трансфера.
     * Уведомляет систему об изменении состояния операции перевода средств.
     *
     * @param transferId уникальный идентификатор трансфера
     * @param correlationId идентификатор корреляции для отслеживания цепочки событий
     * @param status новый статус трансфера (COMPLETED, FAILED, COMPENSATED и т.д.)
     * @param reason причина изменения статуса (опционально)
     * @throws RuntimeException если публикация события завершилась ошибкой
     */
    @Override
    public void publishTransferStatus(String transferId, String correlationId,
                                      String status, String reason) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", status);
        payload.put("reason", reason);
        payload.put("transferId", transferId);
        String eventType = getEventTypeForStatus(status);
        KafkaEvent event = KafkaEvent.createWithTransferId(
                eventType, correlationId, transferId, payload);
        sendEvent(topicConfig.getTransferStatusTopic(), transferId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish TRANSFER_STATUS: transferId={}, status={}",
                                transferId, status, exception);
                        throw new RuntimeException("Failed to publish TRANSFER_STATUS", exception);
                    } else {
                        log.info("Successfully published TRANSFER_STATUS: transferId={}, status={}",
                                transferId, status);
                    }
                });
    }

    /**
     * Универсальный метод отправки события в Kafka.
     * Выполняет асинхронную отправку с обработкой результата и детальным логированием.
     *
     * @param topic название топика Kafka для отправки
     * @param key ключ сообщения (обычно transferId)
     * @param event объект события для отправки
     * @return CompletableFuture с результатом отправки
     */
    private CompletableFuture<SendResult<String, Object>> sendEvent(
            String topic, String key, Object event) {
        String eventType = extractEventType(event);
        log.info("Sending Kafka event: {} to topic: {} with key: {}", eventType, topic, key);
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, event);

        // Обработка результата отправки
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to send Kafka event: {} to topic: {}", eventType, topic, exception);
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
     *
     * @param status статус трансфера
     * @return соответствующий тип события Kafka
     */
    private String getEventTypeForStatus(String status) {
        return switch (status.toUpperCase()) {
            case "COMPLETED" -> "TRANSFER_COMPLETED";
            case "FAILED" -> "TRANSFER_FAILED";
            case "COMPENSATED" -> "TRANSFER_COMPENSATED";
            default -> "TRANSFER_STATUS_UPDATE";
        };
    }
}