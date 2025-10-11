package com.bankx.transfer.infrastructure.persistence.repository;

import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.domain.model.TransferStatus;
import com.bankx.transfer.domain.repository.TransferRepository;
import com.bankx.transfer.infrastructure.persistence.entity.TransferEntity;
import com.bankx.transfer.infrastructure.persistence.mapper.TransferMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Реализация порта TransferRepository с использованием JPA.
 * Инфраструктурная реализация доменного интерфейса для работы с переводами.
 * Отвечает за:
 * - Сохранение и извлечение переводов в/из PostgreSQL
 * - Преобразование между доменной моделью и JPA сущностью
 * - Обеспечение транзакционности операций
 * - Логирование операций для observability
 */
@Repository
@Transactional
@RequiredArgsConstructor
@Slf4j
public class JpaTransferRepository implements TransferRepository {
    private final SpringDataTransferRepository springDataRepository;
    private final TransferMapper mapper;

    @Override
    @Transactional
    public Transfer save(Transfer transfer) {
        log.debug("Saving transfer with id: {}, correlationId: {}",
                transfer.getId(), transfer.getCorrelationId());
        TransferEntity entity = mapper.toEntity(transfer);
        TransferEntity savedEntity = springDataRepository.save(entity);
        log.info("Transfer saved successfully - id: {}, status: {}",
                savedEntity.getId(), savedEntity.getStatus());
        return mapper.toDomain(savedEntity);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Transfer> findById(UUID id) {
        log.debug("Finding transfer by id: {}", id);
        return springDataRepository.findById(id)
                .map(entity -> {
                    log.debug("Transfer found by id: {}", id);
                    return mapper.toDomain(entity);
                })
                .or(() -> {
                    log.debug("Transfer not found by id: {}", id);
                    return Optional.empty();
                });
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Transfer> findByCorrelationId(UUID correlationId) {
        log.debug("Finding transfer by correlationId: {}", correlationId);
        return springDataRepository.findByCorrelationId(correlationId)
                .map(entity -> {
                    log.debug("Transfer found by correlationId: {}", correlationId);
                    return mapper.toDomain(entity);
                })
                .or(() -> {
                    log.debug("Transfer not found by correlationId: {}", correlationId);
                    return Optional.empty();
                });
    }

    @Override
    @Transactional(readOnly = true)
    public List<Transfer> findByFromAccount(String accountNumber) {
        log.debug("Finding transfers by fromAccount: {}", accountNumber);
        List<Transfer> transfers = springDataRepository.findByFromAccount(accountNumber)
                .stream()
                .map(mapper::toDomain)
                .collect(Collectors.toList());
        log.debug("Found {} transfers for fromAccount: {}", transfers.size(), accountNumber);
        return transfers;
    }

    @Override
    @Transactional(readOnly = true)
    public List<Transfer> findByToAccount(String accountNumber) {
        log.debug("Finding transfers by toAccount: {}", accountNumber);
        List<Transfer> transfers = springDataRepository.findByToAccount(accountNumber)
                .stream()
                .map(mapper::toDomain)
                .collect(Collectors.toList());
        log.debug("Found {} transfers for toAccount: {}", transfers.size(), accountNumber);
        return transfers;
    }

    @Override
    @Transactional(readOnly = true)
    public List<Transfer> findByStatus(TransferStatus status) {
        log.debug("Finding transfers by status: {}", status);
        List<Transfer> transfers = springDataRepository.findByStatus(status)
                .stream()
                .map(mapper::toDomain)
                .collect(Collectors.toList());
        log.debug("Found {} transfers with status: {}", transfers.size(), status);
        return transfers;
    }

    @Override
    @Transactional(readOnly = true)
    public boolean existsByCorrelationId(UUID correlationId) {
        log.debug("Checking existence by correlationId: {}", correlationId);
        boolean exists = springDataRepository.existsByCorrelationId(correlationId);
        log.debug("Transfer exists by correlationId {}: {}", correlationId, exists);
        return exists;
    }

    @Override
    @Transactional
    public void delete(Transfer transfer) {
        log.debug("Deleting transfer with id: {}", transfer.getId());
        TransferEntity entity = mapper.toEntity(transfer);
        springDataRepository.delete(entity);
        log.info("Transfer deleted successfully - id: {}", transfer.getId());
    }
}