package com.bankx.transfer.application.service;

import com.bankx.transfer.application.port.EventPublisherPort;
import com.bankx.transfer.domain.model.AccountNumber;
import com.bankx.transfer.domain.model.Money;
import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.domain.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Сервис для обработки команд перевода.
 * Содержит бизнес-логику создания и управления переводами.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransferCommandService {
    private final TransferRepository transferRepository;
    private final EventPublisherPort eventPublisherPort;

    /**
     * Создает новый перевод на основе команды из Kafka
     */
    @Transactional
    public void createTransfer(String correlationId,
                               AccountNumber fromAccount,
                               AccountNumber toAccount,
                               Money amount,
                               String description) {
        log.info("Creating transfer: correlationId={}, from={}, to={}, amount={}",
                correlationId, fromAccount.value(), toAccount.value(), amount.toDisplayString());

        // Проверяем, не существует ли уже перевод с таким correlationId (идемпотентность)
        UUID correlationUUID = UUID.fromString(correlationId);
        if (transferRepository.existsByCorrelationId(correlationUUID)) {
            log.warn("Transfer with correlationId {} already exists", correlationId);
            return; // Идемпотентность - игнорируем дубликат
        }

        // Создаем доменный объект перевода
        Transfer transfer = new Transfer(correlationUUID, fromAccount, toAccount, amount, description);

        // Сохраняем перевод
        Transfer savedTransfer = transferRepository.save(transfer);
        log.info("Transfer created successfully: id={}, correlationId={}",
                savedTransfer.getId(), correlationId);

        // Публикуем событие DEBIT_REQUEST для списания средств
        eventPublisherPort.publishDebitRequest(
                savedTransfer.getId().toString(),
                correlationId,
                fromAccount.value(),
                amount.amount(),
                amount.getCurrencyCode()
        );
    }
}