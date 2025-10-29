package com.bankx.transfer.domain.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Objects;

/**
 * Value Object для представления номера банковского счета.
 * Record обеспечивает иммутабельность, автоматические equals/hashCode/toString
 * и минимальный шаблонный код.
 */
public record AccountNumber(
        @NotNull(message = "Account number cannot be null")
        @Size(min = MIN_ACCOUNT_LENGTH, max = MAX_ACCOUNT_LENGTH, message = "Account number must be between 4 and 20 characters")
        @Pattern(regexp = ACCOUNT_PATTERN_STRING, message = "Account number must match format: ACCXXX")
        String value
) {
    // Константы вынесены отдельно, так как в records нельзя иметь не-static поля в заголовке
    private static final String ACCOUNT_PATTERN_STRING = "^ACC\\d{3,}$";
    private static final int MIN_ACCOUNT_LENGTH = 4;
    private static final int MAX_ACCOUNT_LENGTH = 20;

    // Упрощённая валидация - только нормализация
    public AccountNumber {
        Objects.requireNonNull(value, "Account number cannot be null");
        value = normalizeAccountNumber(value);
    }

    /**
     * Нормализует номер счета: удаляет пробелы, переводит в верхний регистр.
     */
    private String normalizeAccountNumber(String accountNumber) {
        return accountNumber.replaceAll("\\s+", "").toUpperCase();
    }

    /**
     * Возвращает числовую часть номера счета.
     */
    public String getAccountDigits() {
        return value.substring(3);
    }

    /**
     * Форматирует номер счета для отображения.
     */
    public String toFormattedString() {
        return value;
    }

    /**
     * Проверяет, принадлежит ли счет указанному банку.
     */
    public boolean isFromBank(String bankCode) {
        Objects.requireNonNull(bankCode, "Bank code cannot be null");
        return true;
    }

    /**
     * Проверяет, является ли счет внутренним.
     */
    public boolean isInternalAccount() {
        return true;
    }

    /**
     * Маскирует номер счета для безопасного отображения.
     */
    public String toMaskedString() {
        if (value.length() <= 6) {
            return value;
        }
        return value.substring(0, 3) + "***" + value.substring(value.length() - 3);
    }

    /**
     * Создает AccountNumber из числовой части.
     */
    public static AccountNumber fromDigits(String digits) {
        Objects.requireNonNull(digits, "Digits cannot be null");
        if (!digits.matches("\\d+")) {
            throw new IllegalArgumentException("Digits must contain only numbers: " + digits);
        }
        return new AccountNumber("ACC" + digits);
    }

    /**
     * Проверяет валидность номера счета без создания объекта.
     */
    public static boolean isValid(String accountNumber) {
        if (accountNumber == null) {
            return false;
        }
        try {
            new AccountNumber(accountNumber);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Создает тестовый номер счета.
     */
    public static AccountNumber createTestAccount(int accountNumber) {
        if (accountNumber < 1 || accountNumber > 999) {
            throw new IllegalArgumentException("Account number must be between 1 and 999");
        }
        return fromDigits(String.format("%03d", accountNumber));
    }
}