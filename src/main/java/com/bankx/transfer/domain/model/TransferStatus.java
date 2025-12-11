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
 *
 * Соответствует требованиям ТЗ по управлению состоянием перевода (2.2).
 */
@Getter
public enum TransferStatus {

    /**
     * Начальное состояние - перевод создан, ожидает обработки
     * Исходное состояние после получения TRANSFER_COMMAND
     * Соответствует первому шагу в Saga паттерне.
     */
    PENDING("Перевод ожидает обработки", false),

    /**
     * Средства успешно списаны с исходного счета
     * Состояние после получения DEBIT_CONFIRMED
     * Второй шаг в успешном сценарии Saga.
     */
    DEBITED("Средства списаны с исходного счета", false),

    /**
     * Перевод успешно завершен - средства зачислены на целевой счет
     * Финальное успешное состояние после получения CREDIT_CONFIRMED
     * Конечный шаг успешного сценария Saga.
     */
    COMPLETED("Перевод успешно завершен", true),

    /**
     * Ошибка при списании средств с исходного счета
     * Финальное состояние ошибки после получения DEBIT_FAILED
     * Компенсация не требуется, так как списание не выполнено.
     */
    FAILED("Ошибка при списании средств", true),

    /**
     * Ошибка при зачислении средств на целевой счет - начата компенсация
     * Промежуточное состояние после получения CREDIT_FAILED
     * Запускает компенсационный сценарий Saga.
     */
    COMPENSATING("Ошибка при зачислении, выполняется компенсация", false),

    /**
     * Компенсация выполнена успешно - средства возвращены на исходный счет
     * Финальное компенсированное состояние после успешной компенсации
     * Конечный шаг компенсационного сценария Saga.
     */
    COMPENSATED("Компенсация выполнена успешно", true);

    private final String description;

    /**
     * Флаг, указывающий является ли статус терминальным (финальным).
     * В терминальных статусах перевод больше не может менять состояние.
     * Терминальные статусы: COMPLETED, FAILED, COMPENSATED.
     */
    private final boolean terminal;

    /**
     * Конструктор enum.
     *
     * @param description человекочитаемое описание статуса
     * @param terminal флаг терминального статуса
     */
    TransferStatus(String description, boolean terminal) {
        this.description = description;
        this.terminal = terminal;
    }

    /**
     * Проверяет, является ли статус терминальным.
     * Используется для проверки, можно ли дальше изменять состояние перевода.
     *
     * @return true если статус терминальный, false в противном случае
     */
    public boolean isTerminal() {
        return terminal;
    }

    /**
     * Статический метод для проверки, является ли статус терминальным.
     * Убрали для устранения конфликта с методом экземпляра.
     *
     * @param status статус для проверки
     * @return true если статус терминальный, false в противном случае
     */
    public static boolean isTerminalStatus(TransferStatus status) {
        return status != null && status.isTerminal();
    }

    /**
     * Проверяет, является ли статус успешным финальным состоянием.
     * Успешные финальные статусы: COMPLETED.
     *
     * @return true если статус COMPLETED, false в противном случае
     */
    public boolean isSuccessfullyCompleted() {
        return this == COMPLETED;
    }

    /**
     * Проверяет, является ли статус состоянием ошибки.
     * Статусы ошибок: FAILED, COMPENSATING, COMPENSATED.
     *
     * @return true если статус указывает на ошибку, false в противном случае
     */
    public boolean isFailed() {
        return this == FAILED || this == COMPENSATING || this == COMPENSATED;
    }

    /**
     * Проверяет, требуется ли компенсация в текущем статусе.
     * Компенсация требуется только в статусе COMPENSATING.
     *
     * @return true если требуется компенсация, false в противном случае
     */
    public boolean requiresCompensation() {
        return this == COMPENSATING;
    }

    /**
     * Проверяет, можно ли перейти из текущего статуса в целевой.
     * Реализует матрицу допустимых переходов согласно бизнес-логике Saga.
     *
     * @param targetStatus целевой статус для перехода
     * @return true если переход допустим, false в противном случае
     */
    public boolean canTransitionTo(TransferStatus targetStatus) {
        return ALLOWED_TRANSITIONS.contains(new StatusTransition(this, targetStatus));
    }

    /**
     * Получает следующий ожидаемый статус после успешного выполнения операции.
     * Используется для предсказания следующего состояния в Saga.
     *
     * @param currentStatus текущий статус перевода
     * @return следующий статус после успеха
     * @throws IllegalStateException если нельзя определить следующий статус
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
     * Используется для определения состояния при ошибках в Saga.
     *
     * @param currentStatus текущий статус перевода
     * @return статус после неудачи
     * @throws IllegalStateException если нельзя определить статус после неудачи
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
     * Используется для мониторинга и отчетности.
     *
     * @return список активных статусов
     */
    public static List<TransferStatus> getActiveStatuses() {
        return Arrays.stream(values())
                .filter(status -> !status.isTerminal())
                .toList();
    }

    /**
     * Получает все терминальные статусы.
     * Используется для отчетности и анализа завершенных операций.
     *
     * @return список терминальных статусов
     */
    public static List<TransferStatus> getTerminalStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isTerminal)
                .toList();
    }

    /**
     * Получает статусы, требующие ручного вмешательства.
     * В текущей реализации только COMPENSATING требует внимания.
     *
     * @return список статусов, требующих внимания
     */
    public static List<TransferStatus> getStatusesRequiringAttention() {
        return List.of(COMPENSATING);
    }

    /**
     * Внутренний класс для представления допустимых переходов между статусами.
     * Реализует record для неизменяемости и простоты.
     */
    private record StatusTransition(TransferStatus from, TransferStatus to) {
    }

    /**
     * Матрица допустимых переходов между статусами согласно бизнес-логике Saga.
     * Гарантирует целостность состояния переводов.
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

    /**
     * Возвращает строковое представление статуса.
     * Формат: "NAME (description)"
     *
     * @return форматированное строковое представление
     */
    @Override
    public String toString() {
        return name() + " (" + description + ")";
    }

    /**
     * Создает экземпляр enum из строки (case-insensitive).
     * Используется при десериализации из внешних источников.
     *
     * @param value строковое значение статуса
     * @return соответствующий TransferStatus
     * @throws IllegalArgumentException если значение неизвестно
     */
    public static TransferStatus fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Status value cannot be null");
        }
        try {
            return TransferStatus.valueOf(value.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown transfer status: " + value, e);
        }
    }

    /**
     * Проверяет, является ли строка валидным статусом.
     * Используется для валидации входных данных.
     *
     * @param value строковое значение для проверки
     * @return true если значение валидно, false в противном случае
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