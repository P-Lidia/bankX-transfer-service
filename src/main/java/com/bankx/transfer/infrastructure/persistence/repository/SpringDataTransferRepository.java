package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.TransferStatus;
import com.bankx.transfer.infrastructure.persistence.entity.TransferEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA репозиторий для сущности TransferEntity.
 * Предоставляет базовые CRUD операции и кастомные запросы для работы с таблицей transfers.
 * Особенности:
 * - Наследование от JpaRepository для стандартных операций
 * - Поддержка кастомных JPQL запросов
 * - Оптимизированные запросы с использованием индексов
 * - Поддержка пагинации и сортировки при необходимости
 */
@Repository
public interface SpringDataTransferRepository extends JpaRepository<TransferEntity, UUID> {

    Optional<TransferEntity> findByCorrelationId(UUID correlationId);

    List<TransferEntity> findByFromAccount(String fromAccount);

    List<TransferEntity> findByToAccount(String toAccount);

    List<TransferEntity> findByStatus(TransferStatus status);

    boolean existsByCorrelationId(UUID correlationId);

    @Query("SELECT t FROM TransferEntity t WHERE t.createdAt BETWEEN :startDate AND :endDate")
    List<TransferEntity> findByCreatedAtBetween(@Param("startDate") LocalDateTime startDate,
                                                @Param("endDate") LocalDateTime endDate);

    @Query("SELECT t FROM TransferEntity t WHERE t.status = :status AND t.amount BETWEEN :minAmount AND :maxAmount")
    List<TransferEntity> findByStatusAndAmountBetween(@Param("status") TransferStatus status,
                                                      @Param("minAmount") BigDecimal minAmount,
                                                      @Param("maxAmount") BigDecimal maxAmount);
}