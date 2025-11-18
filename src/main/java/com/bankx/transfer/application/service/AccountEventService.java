package com.bankx.transfer.application.service;

import com.bankx.transfer.domain.model.ProcessedEvent;
import com.bankx.transfer.domain.model.ProcessedEventType;
import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.domain.repository.ProcessedEventRepository;
import com.bankx.transfer.domain.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Сервис для обработки событий от Account Service.
 * Получение status = DEBITED, публикация события в топик account.credit.request
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AccountEventService {

    private final TransferRepository transferRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventService outboxEventService;

    /**
     * Обработка подтверждения успешного списания средств.
     * Обновляет статус перевода на DEBITED и публикует CREDIT_REQUEST.
     */
    @Transactional
    public void processDebitConfirmed(String eventId, String correlationId,
                                      UUID transferId, String debitTransactionId) {
        log.info("Processing DEBIT_CONFIRMED: eventId={}, transferId={}, debitTransactionId={}",
                eventId, transferId, debitTransactionId);

        // Проверка идемпотентности
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }
        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на DEBITED
        transfer.processDebitConfirmed(debitTransactionId);
        Transfer savedTransfer = transferRepository.save(transfer);

        // Сохраняем факт обработки события
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.DEBIT_CONFIRMED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие CREDIT_REQUEST для зачисления средств на целевой счет
        publishCreditRequest(savedTransfer, correlationId);

        log.info("Successfully processed DEBIT_CONFIRMED and published CREDIT_REQUEST: transferId={}", transferId);
    }

    /**
     * Публикация события CREDIT_REQUEST через Outbox Pattern
     */
    private void publishCreditRequest(Transfer transfer, String correlationId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("accountId", transfer.getToAccount().value());
        payload.put("amount", transfer.getAmount().amount());
        payload.put("currency", transfer.getAmount().getCurrencyCode());

        outboxEventService.createOutboxEvent(
                "Transfer",
                transfer.getId(),
                "CREDIT_REQUEST",
                payload,
                correlationId
        );
    }

    // todo - другие методы - failed, completed
}