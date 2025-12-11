package com.bankx.transfer.domain.model;

import lombok.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Доменная модель перевода между счетами.
 * Основная бизнес-сущность, содержащая логику управления состоянием перевода.
 * Отвечает за:
 * - Управление жизненным циклом и состояниями перевода
 * - Обеспечение целостности данных перевода
 * - Генерацию событий домена для интеграции с другими сервисами
 */
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class Transfer {
    @EqualsAndHashCode.Include
    private final UUID id;
    private final UUID correlationId;
    private final String fromAccount;
    private final String toAccount;
    private final Money amount;
    private final String description;
    @Setter(AccessLevel.PRIVATE)
    private TransferStatus status;
    private final LocalDateTime createdAt;
    @Setter(AccessLevel.PRIVATE)
    private LocalDateTime updatedAt;
    @Setter(AccessLevel.PRIVATE)
    private LocalDateTime completedAt;
    @Setter(AccessLevel.PRIVATE)
    private String debitTransactionId;
    @Setter(AccessLevel.PRIVATE)
    private String creditTransactionId;
    private final List<TransferEvent> events = new ArrayList<>();

    // Конструктор для создания нового перевода
    public Transfer(UUID correlationId, String fromAccount, String toAccount,
                    Money amount, String description) {
        this.id = UUID.randomUUID();
        this.correlationId = correlationId;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.description = description;
        this.status = TransferStatus.PENDING;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.transferCreated(
                fromAccount,
                toAccount,
                amount.toDisplayString()
        ));
    }

    // Конструктор для восстановления из БД
    @Builder(builderClassName = "RestoreBuilder", builderMethodName = "restoreBuilder")
    private Transfer(UUID id, UUID correlationId, String fromAccount, String toAccount,
                     Money amount, String description, TransferStatus status,
                     LocalDateTime createdAt, LocalDateTime updatedAt, LocalDateTime completedAt,
                     String debitTransactionId, String creditTransactionId) {
        this.id = id;
        this.correlationId = correlationId;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.description = description;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.completedAt = completedAt;
        this.debitTransactionId = debitTransactionId;
        this.creditTransactionId = creditTransactionId;
    }

    /**
     * Обработка подтверждения списания средств.
     * @param transactionId идентификатор транзакции списания
     */
    public void processDebitConfirmed(String transactionId) {
        if (status != TransferStatus.PENDING) {
            throw new IllegalStateException("Cannot confirm debit for transfer in state: " + status);
        }
        this.debitTransactionId = transactionId;
        this.status = TransferStatus.DEBITED;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.debitConfirmed(transactionId));
    }

    /**
     * Обработка ошибки списания средств.
     */
    public void processDebitFailed() {
        if (status != TransferStatus.PENDING) {
            throw new IllegalStateException("Cannot fail debit for transfer in state: " + status);
        }
        this.status = TransferStatus.FAILED;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.debitFailed("Debit operation failed"));
    }

    /**
     * Обработка ошибки списания средств с указанием причины.
     */
    public void processDebitFailed(String reason) {
        if (status != TransferStatus.PENDING) {
            throw new IllegalStateException("Cannot fail debit for transfer in state: " + status);
        }
        this.status = TransferStatus.FAILED;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.debitFailed(reason));
    }

    /**
     * Обработка подтверждения зачисления средств.
     * @param transactionId идентификатор транзакции зачисления
     */
    public void processCreditConfirmed(String transactionId) {
        if (status != TransferStatus.DEBITED) {
            throw new IllegalStateException("Cannot confirm credit for transfer in state: " + status);
        }
        this.creditTransactionId = transactionId;
        this.status = TransferStatus.COMPLETED;
        this.completedAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.creditConfirmed(transactionId));
    }

    /**
     * Обработка ошибки зачисления средств.
     */
    public void processCreditFailed() {
        if (status != TransferStatus.DEBITED) {
            throw new IllegalStateException("Cannot fail credit for transfer in state: " + status);
        }
        this.status = TransferStatus.COMPENSATING;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.creditFailed("Credit operation failed, initiating compensation"));
    }

    /**
     * Обработка ошибки зачисления средств с указанием причины.
     */
    public void processCreditFailed(String reason) {
        if (status != TransferStatus.DEBITED) {
            throw new IllegalStateException("Cannot fail credit for transfer in state: " + status);
        }
        this.status = TransferStatus.COMPENSATING;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.creditFailed(reason));
    }

    /**
     * Обработка успешной компенсации.
     */
    public void processCompensateConfirmed() {
        if (status != TransferStatus.COMPENSATING) {
            throw new IllegalStateException("Cannot complete compensation for transfer in state: " + status);
        }
        this.status = TransferStatus.COMPENSATED;
        this.updatedAt = LocalDateTime.now();
        addEvent(TransferEvent.compensationCompleted());
    }

    /**
     * Добавление события в историю перевода.
     */
    private void addEvent(TransferEvent event) {
        this.events.add(event);
    }

    /**
     * Record для представления событий перевода.
     * Иммутабельный класс для хранения информации о событиях в жизненном цикле перевода.
     */
    public record TransferEvent(
            TransferEventType type,
            LocalDateTime timestamp,
            String message
    ) {
        /**
         * Компактный конструктор для валидации.
         */
        public TransferEvent {
            Objects.requireNonNull(type, "Event type cannot be null");
            Objects.requireNonNull(timestamp, "Timestamp cannot be null");
            Objects.requireNonNull(message, "Message cannot be null");
            if (message.isBlank()) {
                throw new IllegalArgumentException("Message cannot be blank");
            }
        }

        /**
         * Создает новое событие с текущим временем.
         */
        public static TransferEvent now(TransferEventType type, String message) {
            return new TransferEvent(type, LocalDateTime.now(), message);
        }

        /**
         * Создает событие создания перевода.
         */
        public static TransferEvent transferCreated(String fromAccount, String toAccount, String amount) {
            String message = String.format(
                    "Transfer created from %s to %s for %s",
                    fromAccount, toAccount, amount
            );
            return now(TransferEventType.TRANSFER_CREATED, message);
        }

        /**
         * Создает событие подтверждения списания.
         */
        public static TransferEvent debitConfirmed(String transactionId) {
            String message = String.format("Debit confirmed with transaction: %s", transactionId);
            return now(TransferEventType.DEBIT_CONFIRMED, message);
        }

        /**
         * Создает событие ошибки списания.
         */
        public static TransferEvent debitFailed(String reason) {
            String message = String.format("Debit operation failed: %s", reason);
            return now(TransferEventType.DEBIT_FAILED, message);
        }

        /**
         * Создает событие подтверждения зачисления.
         */
        public static TransferEvent creditConfirmed(String transactionId) {
            String message = String.format("Credit confirmed with transaction: %s", transactionId);
            return now(TransferEventType.CREDIT_CONFIRMED, message);
        }

        /**
         * Создает событие ошибки зачисления.
         */
        public static TransferEvent creditFailed(String reason) {
            String message = String.format("Credit operation failed: %s", reason);
            return now(TransferEventType.CREDIT_FAILED, message);
        }

        /**
         * Создает событие завершения компенсации.
         */
        public static TransferEvent compensationCompleted() {
            return now(TransferEventType.COMPENSATION_COMPLETED, "Compensation completed successfully");
        }

        /**
         * Проверяет, является ли событие терминальным (завершающим перевод).
         */
        public boolean isTerminal() {
            return type == TransferEventType.CREDIT_CONFIRMED ||
                    type == TransferEventType.DEBIT_FAILED ||
                    type == TransferEventType.COMPENSATION_COMPLETED;
        }

        /**
         * Проверяет, является ли событие ошибкой.
         */
        public boolean isError() {
            return type == TransferEventType.DEBIT_FAILED ||
                    type == TransferEventType.CREDIT_FAILED;
        }
    }

    @Getter
    public enum TransferEventType {
        TRANSFER_CREATED("Transfer created"),
        DEBIT_CONFIRMED("Debit confirmed"),
        DEBIT_FAILED("Debit failed"),
        CREDIT_CONFIRMED("Credit confirmed"),
        CREDIT_FAILED("Credit failed"),
        COMPENSATION_COMPLETED("Compensation completed");

        private final String description;

        TransferEventType(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return name() + " (" + description + ")";
        }
    }
}