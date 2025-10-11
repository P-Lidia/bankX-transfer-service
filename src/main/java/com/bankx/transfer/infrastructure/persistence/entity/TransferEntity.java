package com.bankx.transfer.infrastructure.persistence.entity;

import com.bankx.transfer.domain.model.TransferStatus;
import com.bankx.transfer.shared.validation.PositiveAmount;
import com.bankx.transfer.shared.validation.ValidCurrency;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * JPA сущность для хранения переводов в PostgreSQL.
 */
@Entity
@Table(name = "transfers", indexes = {
        @Index(name = "idx_transfers_correlation_id", columnList = "correlation_id"),
        @Index(name = "idx_transfers_status", columnList = "status"),
        @Index(name = "idx_transfers_created_at", columnList = "created_at"),
        @Index(name = "idx_transfers_from_account", columnList = "from_account"),
        @Index(name = "idx_transfers_to_account", columnList = "to_account")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class TransferEntity {

    @Id
    @Column(name = "id", columnDefinition = "UUID")
    @EqualsAndHashCode.Include
    private UUID id;

    @NotNull
    @Column(name = "correlation_id", columnDefinition = "UUID", nullable = false)
    private UUID correlationId;

    @NotNull
    @Column(name = "from_account", nullable = false)
    private String fromAccount;

    @NotNull
    @Column(name = "to_account", nullable = false)
    private String toAccount;

    @NotNull
    @PositiveAmount
    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @NotNull
    @ValidCurrency
    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @NotNull
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private TransferStatus status;

    @Column(name = "description", length = 500)
    private String description;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "debit_transaction_id")
    private String debitTransactionId;

    @Column(name = "credit_transaction_id")
    private String creditTransactionId;
}