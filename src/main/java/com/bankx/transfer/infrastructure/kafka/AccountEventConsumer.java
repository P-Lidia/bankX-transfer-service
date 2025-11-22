package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.service.AccountEventService;
import com.bankx.transfer.infrastructure.kafka.dto.AccountEventMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer для обработки событий от Account Service.
 * Получение status = DEBITED, публикация события в топик account.credit.request
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AccountEventConsumer {

    private final AccountEventService accountEventService;

    /**
     * Обработка DEBIT_CONFIRMED из топика account.debit.response
     */
    @KafkaListener(
            topics = "account.debit.response",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "accountEventKafkaListenerContainerFactory"
    )
    public void handleDebitResponse(Acknowledgment acknowledgment,
                                    @Payload AccountEventMessage message) {
        try {
            log.info("=== KAFKA MESSAGE RECEIVED ===");
            log.info("Event ID: {}", message.getEventId());
            log.info("Event Type: {}", message.getEventType());
            log.info("Correlation ID: {}", message.getCorrelationId());
            log.info("Timestamp: {}", message.getTimestamp());

            if (message.getPayload() != null) {
                log.info("Transfer ID: {}", message.getPayload().getTransferId());
                log.info("Debit Transaction ID: {}", message.getPayload().getDebitTransactionId());
                log.info("Account: {}", message.getPayload().getAccount());
            } else {
                log.warn("Payload is null!");
            }
            log.info("=== END KAFKA MESSAGE ===");

            // Валидация
            if (message.getEventId() == null || message.getPayload() == null ||
                    message.getPayload().getTransferId() == null) {
                log.error("Invalid message format - missing required fields");
                throw new IllegalArgumentException("Invalid message format");
            }

            if ("DEBIT_CONFIRMED".equals(message.getEventType())) {
                log.info("Processing DEBIT_CONFIRMED for transfer: {}", message.getPayload().getTransferId());
                accountEventService.processDebitConfirmed(
                        message.getEventId(),
                        message.getCorrelationId(),
                        message.getPayload().getTransferId(),
                        message.getPayload().getDebitTransactionId()
                );
                log.info("Successfully processed DEBIT_CONFIRMED for transfer: {}", message.getPayload().getTransferId());
            } else {
                log.info("Ignoring event type: {}", message.getEventType());
            }

            acknowledgment.acknowledge();
            log.info("Message acknowledged successfully");

        } catch (Exception e) {
            log.error("Failed to process debit response. Message: {}", message, e);
            // Не подтверждаем сообщение для повторной обработки
            throw e;
        }
    }
}