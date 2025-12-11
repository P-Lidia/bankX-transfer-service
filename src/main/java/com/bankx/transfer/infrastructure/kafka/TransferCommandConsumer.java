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

/**
 * Consumer для обработки команд на создание переводов из Kafka.
 * Отвечает за получение TRANSFER_COMMAND событий и создание переводов.
 * Реализует идемпотентность обработки через таблицу processed_events.
 * Каждая команда регистрируется в системе для предотвращения повторной обработки.
 *
 * Соответствует требованиям ТЗ:
 * - Идемпотентность обработки (2.1)
 * - Надежность через ручное подтверждение (manual acknowledgment)
 * - Валидация входных данных
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransferCommandConsumer {
    private final TransferCommandService transferCommandService;

    /**
     * Обрабатывает команды на создание переводов из топика transfer.command
     * Реализует идемпотентность через проверку correlationId в сервисном слое.
     * Каждая команда валидируется перед обработкой.
     *
     * @param acknowledgment механизм подтверждения обработки сообщения Kafka
     * @param message сообщение с командой создания перевода
     *
     * @implNote Метод выполняет следующие шаги:
     * 1. Валидация обязательных полей сообщения
     * 2. Создание доменных объектов (AccountNumber, Money)
     * 3. Вызов сервиса для создания перевода (с проверкой идемпотентности)
     * 4. Подтверждение успешной обработки сообщения
     * При любой ошибке сообщение не подтверждается и может быть повторно обработано
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

            // Создание доменных объектов для валидации
            // AccountNumber выполняет собственную валидацию формата номера счета
            AccountNumber fromAccount = new AccountNumber(message.getFromAccount());
            AccountNumber toAccount = new AccountNumber(message.getToAccount());

            // Money выполняет валидацию суммы и валюты
            // Используем статический метод of для создания Money
            Money amount = Money.of(message.getAmount(), message.getCurrency());

            // Обработка команды через сервис
            // Сервис проверяет идемпотентность по correlationId
            // Преобразуем AccountNumber в строки для передачи в сервис
            transferCommandService.createTransfer(
                    message.getCorrelationId(),
                    fromAccount.getValue(),  // Используем getValue() для получения строки
                    toAccount.getValue(),    // Используем getValue() для получения строки
                    amount,
                    message.getDescription()
            );

            log.info("Successfully processed TRANSFER_COMMAND: correlationId={}",
                    message.getCorrelationId());

            // Подтверждаем обработку сообщения только после успешного сохранения
            acknowledgment.acknowledge();

        } catch (IllegalArgumentException e) {
            // Ошибки валидации - логируем, но подтверждаем сообщение, так как повторная обработка не поможет
            log.error("Validation error for TRANSFER_COMMAND: correlationId={}, error: {}",
                    message.getCorrelationId(), e.getMessage(), e);
            acknowledgment.acknowledge(); // Подтверждаем, чтобы не застрять в бесконечном цикле
            throw e;
        } catch (Exception e) {
            log.error("Failed to process TRANSFER_COMMAND: correlationId={}, error: {}",
                    message.getCorrelationId(), e.getMessage(), e);
            // Не подтверждаем сообщение - оно будет обработано повторно
            // В реальном приложении можно добавить retry и dead letter queue
            throw e;
        }
    }

    /**
     * Выполняет базовую валидацию сообщения с командой перевода.
     * Проверяет наличие обязательных полей и их корректность.
     *
     * @param message сообщение для валидации
     * @throws IllegalArgumentException если сообщение не проходит валидацию
     */
    private void validateMessage(TransferCommandMessage message) {
        if (message.getCorrelationId() == null) {
            throw new IllegalArgumentException("correlationId cannot be null");
        }
        if (message.getFromAccount() == null || message.getFromAccount().trim().isEmpty()) {
            throw new IllegalArgumentException("fromAccount cannot be null or empty");
        }
        if (message.getToAccount() == null || message.getToAccount().trim().isEmpty()) {
            throw new IllegalArgumentException("toAccount cannot be null or empty");
        }
        if (message.getAmount() == null || message.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
        if (message.getCurrency() == null || message.getCurrency().trim().isEmpty()) {
            throw new IllegalArgumentException("currency cannot be null or empty");
        }

        // Дополнительная валидация валюты
        try {
            Currency.getInstance(message.getCurrency().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid currency code: " + message.getCurrency(), e);
        }
    }

    /**
     * Обрабатывает сообщения из Dead Letter Queue для TRANSFER_COMMAND.
     * Используется для обработки сообщений, которые не удалось обработать после нескольких попыток.
     *
     * @param acknowledgment механизм подтверждения обработки
     * @param message сообщение из DLQ
     */
    @KafkaListener(
            topics = "#{@kafkaTopicConfig.getTransferCommandTopic()}.dlq",
            groupId = "${spring.kafka.consumer.group-id}-dlq",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleTransferCommandDLQ(Acknowledgment acknowledgment,
                                         @Payload TransferCommandMessage message) {
        log.warn("Processing TRANSFER_COMMAND from DLQ: correlationId={}, fromAccount={}, toAccount={}",
                message.getCorrelationId(), message.getFromAccount(), message.getToAccount());

        try {
            // Логика обработки сообщений из DLQ:
            // 1. Логирование для дальнейшего анализа
            // 2. Уведомление администраторов
            // 3. Попытка восстановления или ручной обработки

            // В данном случае просто логируем и подтверждаем
            acknowledgment.acknowledge();

            log.warn("Processed message from DLQ: correlationId={}", message.getCorrelationId());

        } catch (Exception e) {
            log.error("Failed to process message from DLQ: correlationId={}, error: {}",
                    message.getCorrelationId(), e.getMessage(), e);
            // Не подтверждаем, чтобы можно было проанализировать
            throw e;
        }
    }

    /**
     * Выполняет расширенную валидацию номера счета.
     * Проверяет формат и допустимость номера счета.
     *
     * @param accountNumber номер счета для валидации
     * @return true если номер счета валиден, false в противном случае
     */
    private boolean isValidAccountNumber(String accountNumber) {
        if (accountNumber == null || accountNumber.trim().isEmpty()) {
            return false;
        }

        // Проверяем базовый формат: ACC + цифры
        return accountNumber.matches("^ACC\\d{3,}$");
    }

    /**
     * Проверяет, являются ли счета одинаковыми.
     * Предотвращает перевод с счета на тот же самый счет.
     *
     * @param fromAccount номер счета отправителя
     * @param toAccount номер счета получателя
     * @return true если счета разные, false если одинаковые
     */
    private boolean areAccountsDifferent(String fromAccount, String toAccount) {
        if (fromAccount == null || toAccount == null) {
            return true; // Пропускаем валидацию, если что-то null - это будет поймано в основной валидации
        }
        return !fromAccount.equalsIgnoreCase(toAccount);
    }
}