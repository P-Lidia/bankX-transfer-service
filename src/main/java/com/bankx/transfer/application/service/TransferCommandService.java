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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransferCommandService {
    private final TransferRepository transferRepository;
    private final OutboxEventService outboxEventService;

    @Transactional
    public void createTransfer(UUID correlationId,
                               AccountNumber fromAccount,
                               AccountNumber toAccount,
                               Money amount,
                               String description) {
        log.info("Creating transfer: correlationId={}, from={}, to={}, amount={}",
                correlationId, fromAccount.value(), toAccount.value(), amount.toDisplayString());

        // Проверяем, не существует ли уже перевод с таким correlationId (идемпотентность)
        if (transferRepository.existsByCorrelationId(correlationId)) {
            log.warn("Transfer with correlationId {} already exists", correlationId);
            return; // Идемпотентность - игнорируем дубликат
        }

        // Создаем доменный объект перевода
        Transfer transfer = new Transfer(correlationId, fromAccount, toAccount, amount, description);

        // Сохраняем перевод
        Transfer savedTransfer = transferRepository.save(transfer);
        log.info("Transfer created successfully: id={}, correlationId={}",
                savedTransfer.getId(), correlationId);

        // Публикуем событие DEBIT_REQUEST для списание средств через Outbox Pattern
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", fromAccount.value());
        payload.put("amount", amount.amount());
        payload.put("currency", amount.getCurrencyCode());
        payload.put("transferId", savedTransfer.getId().toString());
        payload.put("correlationId", correlationId.toString());
        outboxEventService.createOutboxEvent(
                "Transfer",
                savedTransfer.getId(),
                "DEBIT_REQUEST",
                payload,
                correlationId
        );

        log.debug("DEBIT_REQUEST outbox event created for transfer: {}", savedTransfer.getId());
    }
}