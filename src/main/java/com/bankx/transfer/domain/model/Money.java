package com.bankx.transfer.domain.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.Objects;

/**
 * Value Object для представления денежных сумм с валютой.
 * Record обеспечивает иммутабельность, автоматические equals/hashCode/toString
 * и минимальный шаблонный код.
 * Отвечает за:
 * - Представление денежных сумм с валютой
 * - Безопасные арифметические операции с деньгами
 * - Контроль округления и точности
 * - Соответствие стандартам финансовых расчетов
 */
public record Money(
        @NotNull(message = "Amount cannot be null")
        @DecimalMin(value = "0.0", inclusive = false, message = "Amount must be positive")
        @Digits(integer = 15, fraction = 2, message = "Amount must have max 15 digits and 2 decimals")
        BigDecimal amount,
        @NotNull(message = "Currency cannot be null")
        Currency currency
) {
    private static final int DEFAULT_SCALE = 2;
    private static final RoundingMode DEFAULT_ROUNDING = RoundingMode.HALF_EVEN;

    // Компактный конструктор для валидации и нормализации
    public Money {
        Objects.requireNonNull(amount, "Amount cannot be null");
        Objects.requireNonNull(currency, "Currency cannot be null");
        // Нормализуем масштаб согласно валюте
        int currencyScale = currency.getDefaultFractionDigits();
        if (currencyScale < 0) {
            currencyScale = DEFAULT_SCALE;
        }
        amount = amount.setScale(currencyScale, DEFAULT_ROUNDING);
    }

    /**
     * Создает новый объект Money из строки.
     *
     * @param amount денежная сумма в виде строки
     * @param currencyCode код валюты (ISO 4217)
     * @throws IllegalArgumentException если параметры невалидны
     */
    public Money(String amount, String currencyCode) {
        this(parseAmount(amount), parseCurrency(currencyCode));
    }

    private static BigDecimal parseAmount(String amount) {
        Objects.requireNonNull(amount, "Amount cannot be null");
        try {
            return new BigDecimal(amount.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid amount format: " + amount, e);
        }
    }

    private static Currency parseCurrency(String currencyCode) {
        Objects.requireNonNull(currencyCode, "Currency code cannot be null");
        try {
            return Currency.getInstance(currencyCode.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid currency code: " + currencyCode, e);
        }
    }

    /**
     * Проверяет, является ли сумма отрицательной или нулевой.
     */
    public boolean isNegativeOrZero() {
        return amount.compareTo(BigDecimal.ZERO) <= 0;
    }

    /**
     * Проверяет, является ли сумма положительной.
     */
    public boolean isPositive() {
        return amount.compareTo(BigDecimal.ZERO) > 0;
    }

    /**
     * Проверяет, является ли сумма нулевой.
     */
    public boolean isZero() {
        return amount.compareTo(BigDecimal.ZERO) == 0;
    }

    /**
     * Сравнивает две денежные суммы (должны быть в одной валюте).
     */
    public int compareTo(Money other) {
        Objects.requireNonNull(other, "Cannot compare to null");
        checkSameCurrency(other);
        return this.amount.compareTo(other.amount);
    }

    /**
     * Проверяет, больше ли текущая сумма чем указанная.
     */
    public boolean isGreaterThan(Money other) {
        return compareTo(other) > 0;
    }

    /**
     * Проверяет, меньше ли текущая сумма чем указанная.
     */
    public boolean isLessThan(Money other) {
        return compareTo(other) < 0;
    }

    /**
     * Проверяет, равны ли суммы (включая валюту).
     */
    public boolean isEqualTo(Money other) {
        return Objects.equals(this, other);
    }

    /**
     * Складывает две денежные суммы (должны быть в одной валюте).
     */
    public Money add(Money other) {
        Objects.requireNonNull(other, "Cannot add null money");
        checkSameCurrency(other);
        BigDecimal newAmount = this.amount.add(other.amount);
        return new Money(newAmount, this.currency);
    }

    /**
     * Вычитает денежную сумму (должны быть в одной валюте).
     */
    public Money subtract(Money other) {
        Objects.requireNonNull(other, "Cannot subtract null money");
        checkSameCurrency(other);
        BigDecimal newAmount = this.amount.subtract(other.amount);
        return new Money(newAmount, this.currency);
    }

    /**
     * Умножает денежную сумму на множитель.
     */
    public Money multiply(BigDecimal multiplier) {
        Objects.requireNonNull(multiplier, "Multiplier cannot be null");
        BigDecimal newAmount = this.amount.multiply(multiplier);
        return new Money(newAmount, this.currency);
    }

    /**
     * Умножает денежную сумму на целочисленный множитель.
     */
    public Money multiply(int multiplier) {
        BigDecimal newAmount = this.amount.multiply(new BigDecimal(multiplier));
        return new Money(newAmount, this.currency);
    }

    /**
     * Делит денежную сумму на делитель.
     */
    public Money divide(BigDecimal divisor) {
        Objects.requireNonNull(divisor, "Divisor cannot be null");
        if (divisor.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("Division by zero is not allowed");
        }
        BigDecimal newAmount = this.amount.divide(divisor, DEFAULT_SCALE, DEFAULT_ROUNDING);
        return new Money(newAmount, this.currency);
    }

    /**
     * Делит денежную сумму на целочисленный делитель.
     */
    public Money divide(int divisor) {
        if (divisor == 0) {
            throw new IllegalArgumentException("Division by zero is not allowed");
        }
        BigDecimal newAmount = this.amount.divide(new BigDecimal(divisor), DEFAULT_SCALE, DEFAULT_ROUNDING);
        return new Money(newAmount, this.currency);
    }

    /**
     * Проверяет совпадение валюты.
     */
    private void checkSameCurrency(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(
                    "Currency mismatch: " + this.currency + " vs " + other.currency);
        }
    }

    /**
     * Возвращает нулевую сумму в той же валюте.
     */
    public Money zero() {
        return new Money(BigDecimal.ZERO, this.currency);
    }

    /**
     * Форматирует сумму для отображения.
     */
    public String toDisplayString() {
        return String.format("%s %s", amount, currency.getCurrencyCode());
    }

    /**
     * Форматирует сумму для отображения с символом валюты.
     */
    public String toFormattedString() {
        String symbol;
        try {
            symbol = currency.getSymbol();
        } catch (Exception e) {
            symbol = currency.getCurrencyCode();
        }
        return String.format("%s%s", symbol, amount);
    }

    /**
     * Возвращает код валюты.
     */
    public String getCurrencyCode() {
        return currency.getCurrencyCode();
    }

    /**
     * Возвращает символ валюты.
     */
    public String getCurrencySymbol() {
        return currency.getSymbol();
    }

    /**
     * Проверяет, совпадает ли валюта с указанной.
     */
    public boolean hasCurrency(String currencyCode) {
        Objects.requireNonNull(currencyCode, "Currency code cannot be null");
        return this.currency.getCurrencyCode().equals(currencyCode.toUpperCase().trim());
    }

    /**
     * Создает объект Money из целого числа (для целых сумм).
     */
    public static Money of(long amount, String currencyCode) {
        return new Money(BigDecimal.valueOf(amount), Currency.getInstance(currencyCode));
    }

    /**
     * Создает объект Money из double (только для тестов).
     */
    public static Money of(double amount, String currencyCode) {
        return new Money(BigDecimal.valueOf(amount), Currency.getInstance(currencyCode));
    }

    /**
     * Создает объект Money из BigDecimal.
     */
    public static Money of(BigDecimal amount, String currencyCode) {
        return new Money(amount, Currency.getInstance(currencyCode));
    }

    /**
     * Создает объект Money из строки.
     */
    public static Money of(String amount, String currencyCode) {
        return new Money(amount, currencyCode);
    }

    /**
     * Возвращает нулевую сумму в указанной валюте.
     */
    public static Money zero(String currencyCode) {
        return new Money(BigDecimal.ZERO, Currency.getInstance(currencyCode));
    }

    /**
     * Проверяет валидность денежной суммы.
     */
    public static boolean isValid(String amount, String currencyCode) {
        if (amount == null || currencyCode == null) {
            return false;
        }
        try {
            new Money(amount, currencyCode);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Конвертирует сумму в другую валюту (упрощенная версия).
     */
    public Money convertTo(String targetCurrencyCode, BigDecimal exchangeRate) {
        Objects.requireNonNull(targetCurrencyCode, "Target currency code cannot be null");
        Objects.requireNonNull(exchangeRate, "Exchange rate cannot be null");
        if (exchangeRate.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Exchange rate must be positive");
        }
        Currency targetCurrency = Currency.getInstance(targetCurrencyCode.toUpperCase());
        BigDecimal convertedAmount = this.amount.multiply(exchangeRate);
        return new Money(convertedAmount, targetCurrency);
    }

    /**
     * Возвращает абсолютное значение суммы.
     */
    public Money abs() {
        return new Money(amount.abs(), currency);
    }

    /**
     * Возвращает сумму с противоположным знаком.
     */
    public Money negate() {
        return new Money(amount.negate(), currency);
    }

    @Override
    public String toString() {
        return String.format("Money[amount=%s, currency=%s]", amount, currency.getCurrencyCode());
    }
}