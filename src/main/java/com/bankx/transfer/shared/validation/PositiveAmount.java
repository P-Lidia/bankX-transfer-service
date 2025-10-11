package com.bankx.transfer.shared.validation;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import java.lang.annotation.*;
import java.math.BigDecimal;

/**
 * Валидационная аннотация для проверки положительной суммы.
 */
@Documented
@Constraint(validatedBy = PositiveAmount.AmountValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface PositiveAmount {
    String message() default "Amount must be positive";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    class AmountValidator implements ConstraintValidator<PositiveAmount, BigDecimal> {

        @Override
        public boolean isValid(BigDecimal value, ConstraintValidatorContext context) {
            if (value == null) {
                return true; // null значения обрабатываются @NotNull
            }
            return value.compareTo(BigDecimal.ZERO) > 0;
        }
    }
}