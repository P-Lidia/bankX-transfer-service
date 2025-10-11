package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.AccountNumber;
import com.bankx.transfer.domain.model.Money;
import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.infrastructure.persistence.entity.TransferEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Currency;

/**
 * Маппер для преобразования между доменной моделью Transfer и JPA сущностью TransferEntity.
 */
@Component
@Slf4j
public class TransferMapper {

    public TransferEntity toEntity(Transfer transfer) {
        if (transfer == null) {
            return null;
        }
        log.debug("Mapping Transfer domain to Entity - id: {}", transfer.getId());
        return TransferEntity.builder()
                .id(transfer.getId())
                .correlationId(transfer.getCorrelationId())
                .fromAccount(transfer.getFromAccount().value())
                .toAccount(transfer.getToAccount().value())
                .amount(transfer.getAmount().amount())
                .currency(transfer.getAmount().currency().getCurrencyCode())
                .status(transfer.getStatus())
                .description(transfer.getDescription())
                .createdAt(transfer.getCreatedAt())
                .updatedAt(transfer.getUpdatedAt())
                .completedAt(transfer.getCompletedAt())
                .debitTransactionId(transfer.getDebitTransactionId())
                .creditTransactionId(transfer.getCreditTransactionId())
                .build();
    }

    public Transfer toDomain(TransferEntity entity) {
        if (entity == null) {
            return null;
        }
        log.debug("Mapping TransferEntity to Domain - id: {}", entity.getId());
        AccountNumber fromAccount = new AccountNumber(entity.getFromAccount());
        AccountNumber toAccount = new AccountNumber(entity.getToAccount());
        Money amount = new Money(entity.getAmount(), Currency.getInstance(entity.getCurrency()));
        return Transfer.restoreBuilder()
                .id(entity.getId())
                .correlationId(entity.getCorrelationId())
                .fromAccount(fromAccount)
                .toAccount(toAccount)
                .amount(amount)
                .description(entity.getDescription())
                .status(entity.getStatus())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .completedAt(entity.getCompletedAt())
                .debitTransactionId(entity.getDebitTransactionId())
                .creditTransactionId(entity.getCreditTransactionId())
                .build();
    }
}