package com.bankx.transfer.domain.model;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * Перечисление статусов перевода в рамках Saga паттерна.
 * Определяет жизненный цикл перевода и допустимые переходы между состояниями.
 * Отвечает за:
 * - Управление состояниями перевода согласно паттерну Saga
 * - Валидацию переходов между состояниями
 * - Определение финальных и компенсирующих состояний
 * - Интеграцию с событиями Kafka для координации распределенной транзакции
 */
@Getter
public enum TransferStatus {

    /**
     * Начальное состояние - перевод создан, ожидает обработки
     * Исходное состояние после получения TRANSFER_COMMAND
     */
    PENDING("Перевод ожидает обработки", false),

    /**
     * Средства успешно списаны с исходного счета
     * Состояние после получения DEBIT_CONFIRMED
     */
    DEBITED("Средства списаны с исходного счета", false),

    /**
     * Перевод успешно завершен - средства зачислены на целевой счет
     * Финальное успешное состояние после получения CREDIT_CONFIRMED
     */
    COMPLETED("Перевод успешно завершен", true),

    /**
     * Ошибка при списании средств с исходного счета
     * Финальное состояние ошибки после получения DEBIT_FAILED
     */
    FAILED("Ошибка при списании средств", true),

    /**
     * Ошибка при зачислении средств на целевой счет - начата компенсация
     * Промежуточное состояние после получения CREDIT_FAILED
     */
    COMPENSATING("Ошибка при зачислении, выполняется компенсация", false),

    /**
     * Компенсация выполнена успешно - средства возвращены на исходный счет
     * Финальное компенсированное состояние после успешной компенсации
     */
    COMPENSATED("Компенсация выполнена успешно", true);

    private final String description;
    /**
     * -- GETTER --
     *  Проверяет, является ли статус финальным (терминальным).
     *  В финальных статусах перевод больше не может менять состояние.
     */
    @Getter
    private final boolean terminal;

    TransferStatus(String description, boolean terminal) {
        this.description = description;
        this.terminal = terminal;
    }

    /**
     * Проверяет, является ли статус успешным финальным состоянием.
     */
    public boolean isSuccessfullyCompleted() {
        return this == COMPLETED;
    }

    /**
     * Проверяет, является ли статус состоянием ошибки.
     */
    public boolean isFailed() {
        return this == FAILED || this == COMPENSATING || this == COMPENSATED;
    }

    /**
     * Проверяет, требуется ли компенсация в текущем статусе.
     */
    public boolean requiresCompensation() {
        return this == COMPENSATING;
    }

    /**
     * Проверяет, можно ли перейти из текущего статуса в целевой.
     */
    public boolean canTransitionTo(TransferStatus targetStatus) {
        return ALLOWED_TRANSITIONS.contains(new StatusTransition(this, targetStatus));
    }

    /**
     * Получает следующий ожидаемый статус после успешного выполнения операции.
     */
    public static TransferStatus getNextStatusAfterSuccess(TransferStatus currentStatus) {
        return switch (currentStatus) {
            case PENDING -> DEBITED;
            case DEBITED -> COMPLETED;
            case COMPENSATING -> COMPENSATED;
            default -> throw new IllegalStateException(
                    "Cannot determine next status after success for: " + currentStatus);
        };
    }

    /**
     * Получает статус после неудачного выполнения операции.
     */
    public static TransferStatus getStatusAfterFailure(TransferStatus currentStatus) {
        return switch (currentStatus) {
            case PENDING -> FAILED;
            case DEBITED -> COMPENSATING;
            default -> throw new IllegalStateException(
                    "Cannot determine status after failure for: " + currentStatus);
        };
    }

    /**
     * Получает все не терминальные статусы (активные переводы).
     */
    public static List<TransferStatus> getActiveStatuses() {
        return Arrays.stream(values())
                .filter(status -> !status.isTerminal())
                .toList();
    }

    /**
     * Получает все терминальные статусы.
     */
    public static List<TransferStatus> getTerminalStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isTerminal)
                .toList();
    }

    /**
     * Получает статусы, требующие ручного вмешательства.
     */
    public static List<TransferStatus> getStatusesRequiringAttention() {
        return List.of(COMPENSATING);
    }

    /**
     * Внутренний класс для представления допустимых переходов между статусами.
     */
    private record StatusTransition(TransferStatus from, TransferStatus to) {
    }

    /**
     * Матрица допустимых переходов между статусами согласно бизнес-логике Saga.
     */
    private static final List<StatusTransition> ALLOWED_TRANSITIONS = List.of(
            // PENDING -> DEBITED (успешное списание)
            new StatusTransition(PENDING, DEBITED),
            // PENDING -> FAILED (ошибка списания)
            new StatusTransition(PENDING, FAILED),
            // DEBITED -> COMPLETED (успешное зачисление)
            new StatusTransition(DEBITED, COMPLETED),
            // DEBITED -> COMPENSATING (ошибка зачисления)
            new StatusTransition(DEBITED, COMPENSATING),
            // COMPENSATING -> COMPENSATED (успешная компенсация)
            new StatusTransition(COMPENSATING, COMPENSATED)
    );

    @Override
    public String toString() {
        return name() + " (" + description + ")";
    }

    /**
     * Создает экземпляр enum из строки (case-insensitive).
     */
    public static void fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Status value cannot be null");
        }
        try {
            TransferStatus.valueOf(value.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown transfer status: " + value, e);
        }
    }

    /**
     * Проверяет, является ли строка валидным статусом.
     */
    public static boolean isValidStatus(String value) {
        if (value == null) {
            return false;
        }
        try {
            fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}