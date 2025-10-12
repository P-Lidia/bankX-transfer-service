package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.infrastructure.config.KafkaTopicConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Утилитарный класс для отправки событий Kafka.
 * Инкапсулирует логику отправки с обработкой ошибок.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;
    private final KafkaTopicConfig topicConfig;

    /**
     * Отправка события DEBIT_REQUEST
     */
    public CompletableFuture<SendResult<String, KafkaEvent>> sendDebitRequest(
            String correlationId, String transferId, Map<String, Object> payload) {

        KafkaEvent event = KafkaEvent.createWithTransferId(
                "DEBIT_REQUEST", correlationId, transferId, payload);

        return sendEvent(topicConfig.getAccountDebitRequestTopic(), transferId, event);
    }

    /**
     * Отправка события CREDIT_REQUEST
     */
    public CompletableFuture<SendResult<String, KafkaEvent>> sendCreditRequest(
            String correlationId, String transferId, Map<String, Object> payload) {

        KafkaEvent event = KafkaEvent.createWithTransferId(
                "CREDIT_REQUEST", correlationId, transferId, payload);

        return sendEvent(topicConfig.getAccountCreditRequestTopic(), transferId, event);
    }

    /**
     * Отправка события COMPENSATE_DEBIT
     */
    public CompletableFuture<SendResult<String, KafkaEvent>> sendCompensateDebit(
            String correlationId, String transferId, Map<String, Object> payload) {

        KafkaEvent event = KafkaEvent.createWithTransferId(
                "COMPENSATE_DEBIT", correlationId, transferId, payload);

        return sendEvent(topicConfig.getAccountCompensateDebitTopic(), transferId, event);
    }

    /**
     * Отправка финального статуса перевода
     */
    public CompletableFuture<SendResult<String, KafkaEvent>> sendTransferStatus(
            String eventType, String correlationId, String transferId, Map<String, Object> payload) {

        KafkaEvent event = KafkaEvent.createWithTransferId(
                eventType, correlationId, transferId, payload);

        return sendEvent(topicConfig.getTransferStatusTopic(), transferId, event);
    }

    /**
     * Универсальный метод отправки события
     */
    private CompletableFuture<SendResult<String, KafkaEvent>> sendEvent(
            String topic, String key, KafkaEvent event) {

        log.info("Sending Kafka event: {} to topic: {} with key: {}",
                event.getEventType(), topic, key);

        CompletableFuture<SendResult<String, KafkaEvent>> future =
                kafkaTemplate.send(topic, key, event);

        // Обработка результата отправки
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to send Kafka event: {} to topic: {}",
                        event.getEventType(), topic, exception);
            } else {
                log.debug("Successfully sent Kafka event: {} to topic: {} partition: {} offset: {}",
                        event.getEventType(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}