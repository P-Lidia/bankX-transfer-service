package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.service.AccountEventService;
import com.bankx.transfer.infrastructure.kafka.dto.AccountEventMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Консьюмер для обработки событий от Account Service.
 * Отвечает за прием и обработку ответов на операции списания/зачисления средств.
 * Обеспечивает идемпотентную обработку через таблицу processed_events.
 * Реализует часть Saga паттерна - обработку ответов от Account Service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountEventService accountEventService;

    /**
     * Обрабатывает ответы на запросы списания средств (дебетовые операции).
     * Подписывается на топик account.debit.response для получения уведомлений
     * об успешном списании или ошибке от Account Service.
     *
     * @param message сообщение Kafka с результатом дебетовой операции
     * @param key ключ сообщения (correlationId или transferId)
     * @param acknowledgment объект для подтверждения обработки сообщения
     */
    @KafkaListener(
            topics = "${kafka.topics.account.debit.response}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "accountEventContainerFactory"
    )
    public void handleDebitResponse(@Payload AccountEventMessage message,
                                    @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                    Acknowledgment acknowledgment) {
        try {
            log.info("Received debit response event: eventId={}, eventType={}, correlationId={}",
                    message.getEventId(), message.getEventType(), message.getCorrelationId());

            String eventId = message.getEventId();
            UUID correlationId = message.getCorrelationId();
            UUID transferId = message.getPayload() != null ?
                    message.getPayload().getTransferId() : null;

            if (transferId == null) {
                log.error("TransferId is null in debit response: eventId={}", eventId);
                acknowledgment.acknowledge();
                return;
            }

            switch (message.getEventType()) {
                case "DEBIT_CONFIRMED":
                    accountEventService.processDebitConfirmed(
                            eventId,
                            correlationId,
                            transferId,
                            message.getPayload().getDebitTransactionId()
                    );
                    break;
                case "DEBIT_FAILED":
                    accountEventService.processDebitFailed(
                            eventId,
                            correlationId,
                            transferId,
                            message.getPayload().getReason()
                    );
                    break;
                default:
                    log.warn("Unknown debit event type: {}", message.getEventType());
            }

            acknowledgment.acknowledge();
            log.info("Successfully processed debit response event: eventId={}", eventId);
        } catch (Exception e) {
            log.error("Error processing debit response event: eventId={}",
                    message.getEventId(), e);
            // Можно добавить retry логику или отправить в DLT
        }
    }

    /**
     * Обрабатывает ответы на запросы зачисления средств (кредитовые операции).
     * Подписывается на топик account.credit.response для получения уведомлений
     * об успешном зачислении или ошибке от Account Service.
     *
     * @param message сообщение Kafka с результатом кредитовой операции
     * @param key ключ сообщения (correlationId или transferId)
     * @param acknowledgment объект для подтверждения обработки сообщения
     */
    @KafkaListener(
            topics = "${kafka.topics.account.credit.response}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "accountEventContainerFactory"
    )
    public void handleCreditResponse(@Payload AccountEventMessage message,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                     Acknowledgment acknowledgment) {
        try {
            log.info("Received credit response event: eventId={}, eventType={}, correlationId={}",
                    message.getEventId(), message.getEventType(), message.getCorrelationId());

            String eventId = message.getEventId();
            UUID correlationId = message.getCorrelationId();
            UUID transferId = message.getPayload() != null ?
                    message.getPayload().getTransferId() : null;

            if (transferId == null) {
                log.error("TransferId is null in credit response: eventId={}", eventId);
                acknowledgment.acknowledge();
                return;
            }

            switch (message.getEventType()) {
                case "CREDIT_CONFIRMED":
                    accountEventService.processCreditConfirmed(
                            eventId,
                            correlationId,
                            transferId,
                            message.getPayload().getCreditTransactionId()
                    );
                    break;
                case "CREDIT_FAILED":
                    accountEventService.processCreditFailed(
                            eventId,
                            correlationId,
                            transferId,
                            message.getPayload().getReason()
                    );
                    break;
                default:
                    log.warn("Unknown credit event type: {}", message.getEventType());
            }

            acknowledgment.acknowledge();
            log.info("Successfully processed credit response event: eventId={}", eventId);
        } catch (Exception e) {
            log.error("Error processing credit response event: eventId={}",
                    message.getEventId(), e);
        }
    }
}