package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.service.TransferCommandService;
import com.bankx.transfer.domain.model.AccountNumber;
import com.bankx.transfer.domain.model.Money;
import com.bankx.transfer.infrastructure.kafka.dto.TransferCommandMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Currency;
import java.util.UUID;

/**
 * Consumer для обработки команд на создание переводов из Kafka.
 * Отвечает за получение TRANSFER_COMMAND событий и создание переводов.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransferCommandConsumer {
    private final TransferCommandService transferCommandService;

    /**
     * Обрабатывает команды на создание переводов из топика transfer.command
     */
    @KafkaListener(
            topics = "#{@kafkaTopicConfig.getTransferCommandTopic()}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleTransferCommand(Acknowledgment acknowledgment,
                                      @Payload TransferCommandMessage message) {
        log.info("Received TRANSFER_COMMAND: correlationId={}, fromAccount={}, toAccount={}, amount={}",
                message.getCorrelationId(), message.getFromAccount(),
                message.getToAccount(), message.getAmount());
        try {
            // Валидация обязательных полей
            validateMessage(message);

            // Создание доменных объектов
            AccountNumber fromAccount = new AccountNumber(message.getFromAccount());
            AccountNumber toAccount = new AccountNumber(message.getToAccount());
            Money amount = new Money(message.getAmount(), Currency.getInstance(message.getCurrency()));

            // Обработка команды
            transferCommandService.createTransfer(
                    message.getCorrelationId(),
                    fromAccount,
                    toAccount,
                    amount,
                    message.getDescription()
            );
            log.info("Successfully processed TRANSFER_COMMAND: correlationId={}",
                    message.getCorrelationId());

            // Подтверждаем обработку сообщения
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process TRANSFER_COMMAND: correlationId={}, error: {}",
                    message.getCorrelationId(), e.getMessage(), e);
            throw e;
        }
    }

    private void validateMessage(TransferCommandMessage message) {
        if (message.getCorrelationId() == null) {
            throw new IllegalArgumentException("correlationId cannot be null");
        }
        if (message.getFromAccount() == null) {
            throw new IllegalArgumentException("fromAccount cannot be null");
        }
        if (message.getToAccount() == null) {
            throw new IllegalArgumentException("toAccount cannot be null");
        }
        if (message.getAmount() == null) {
            throw new IllegalArgumentException("amount cannot be null");
        }
        if (message.getCurrency() == null) {
            throw new IllegalArgumentException("currency cannot be null");
        }
    }
}