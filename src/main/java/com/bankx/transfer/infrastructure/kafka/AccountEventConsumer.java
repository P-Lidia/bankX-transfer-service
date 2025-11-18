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
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDebitResponse(Acknowledgment acknowledgment,
                                    @Payload AccountEventMessage message) {
        log.info("Received debit response: eventId={}, eventType={}, correlationId={}",
                message.getEventId(), message.getEventType(), message.getCorrelationId());

        try {
            // Валидация
            if (message.getEventId() == null || message.getPayload() == null ||
                    message.getPayload().getTransferId() == null) {
                throw new IllegalArgumentException("Invalid message format");
            }
            if ("DEBIT_CONFIRMED".equals(message.getEventType())) {
                accountEventService.processDebitConfirmed(
                        message.getEventId(),
                        message.getCorrelationId(),
                        message.getPayload().getTransferId(),
                        message.getPayload().getDebitTransactionId()
                );
            } else {
                // todo Другие события добавить более подробно, сейчас просто логируется
                log.info("Ignoring event type: {} - this is handled by other team members",
                        message.getEventType());
            }

            acknowledgment.acknowledge();
            log.info("Successfully processed debit response: eventId={}", message.getEventId());

        } catch (Exception e) {
            log.error("Failed to process debit response: eventId={}", message.getEventId(), e);
            throw e;
        }
    }

    // todo добавить обработку credit.response
}