package com.bankx.transfer.domain.model;

/**
 * Типы обработанных событий для обеспечения идемпотентности Kafka-консьюмеров.
 * Каждое событие, обработанное сервисом, регистрируется с этим типом
 * для предотвращения повторной обработки.
 * Соответствует требованиям ТЗ по идемпотентности (2.1).
 */
public enum ProcessedEventType {
    TRANSFER_COMMAND("TRANSFER_COMMAND"),
    DEBIT_CONFIRMED("DEBIT_CONFIRMED"),
    DEBIT_FAILED("DEBIT_FAILED"),
    CREDIT_CONFIRMED("CREDIT_CONFIRMED"),
    CREDIT_FAILED("CREDIT_FAILED"),
    COMPENSATE_CONFIRMED("COMPENSATE_CONFIRMED");

    private final String value;

    ProcessedEventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Создает enum из строкового значения.
     * Используется при десериализации из БД.
     *
     * @param value строковое представление типа события
     * @return соответствующий enum
     * @throws IllegalArgumentException если значение неизвестно
     */
    public static ProcessedEventType fromString(String value) {
        for (ProcessedEventType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown ProcessedEventType: " + value);
    }
}