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
 * Обрабатывает ответы на операции списания/зачисления средств в рамках Saga паттерна.
 * Отвечает за:
 * - DEBIT_CONFIRMED: обновление статуса на DEBITED, публикация CREDIT_REQUEST
 * - DEBIT_FAILED: обновление статуса на FAILED, публикация TRANSFER_FAILED
 * - CREDIT_CONFIRMED: обновление статуса на COMPLETED, публикация TRANSFER_COMPLETED
 * - CREDIT_FAILED: обновление статуса на COMPENSATING, публикация COMPENSATE_DEBIT
 * - COMPENSATE_CONFIRMED: обновление статуса на COMPENSATED, публикация TRANSFER_COMPENSATED
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
     * Первый шаг в успешном сценарии Saga.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода
     * @param debitTransactionId идентификатор транзакции списания
     */
    @Transactional
    public void processDebitConfirmed(String eventId, UUID correlationId,
                                      UUID transferId, String debitTransactionId) {
        log.info("Processing DEBIT_CONFIRMED: eventId={}, transferId={}, correlationId={}, debitTransactionId={}",
                eventId, transferId, correlationId, debitTransactionId);

        // Проверка идемпотентности через таблицу processed_events
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }

        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на DEBITED
        transfer.processDebitConfirmed(debitTransactionId);
        Transfer savedTransfer = transferRepository.save(transfer);

        // Сохраняем факт обработки события для идемпотентности
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.DEBIT_CONFIRMED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие CREDIT_REQUEST для зачисления средств на целевой счет
        publishCreditRequest(savedTransfer, correlationId);

        log.info("Successfully processed DEBIT_CONFIRMED and published CREDIT_REQUEST: transferId={}", transferId);
    }

    /**
     * Публикация события CREDIT_REQUEST через Outbox Pattern.
     * Используется после успешного списания для инициации зачисления.
     *
     * @param transfer объект перевода
     * @param correlationId идентификатор корреляции
     */
    private void publishCreditRequest(Transfer transfer, UUID correlationId) {
        outboxEventService.createCreditRequestEvent(
                transfer.getId().toString(),
                correlationId.toString(),
                transfer.getToAccount(),
                transfer.getAmount().getAmount(),
                transfer.getAmount().getCurrencyCode()
        );
    }

    /**
     * Обработка ошибки списания средств.
     * Обновляет статус перевода на FAILED и публикует TRANSFER_FAILED.
     * Финальный шаг в сценарии ошибки списания.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода
     * @param errorReason причина ошибки списания
     */
    @Transactional
    public void processDebitFailed(String eventId, UUID correlationId,
                                   UUID transferId, String errorReason) {
        log.info("Processing DEBIT_FAILED: eventId={}, transferId={}, correlationId={}, errorReason={}",
                eventId, transferId, correlationId, errorReason);

        // Проверка идемпотентности
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }

        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на FAILED
        transfer.processDebitFailed(errorReason);
        transferRepository.save(transfer);

        // Сохраняем факт обработки события
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.DEBIT_FAILED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие TRANSFER_FAILED в топик transfer.status
        outboxEventService.createTransferFailedEvent(
                transferId.toString(),
                correlationId.toString(),
                errorReason
        );

        log.info("Successfully processed DEBIT_FAILED and published TRANSFER_FAILED: transferId={}", transferId);
    }

    /**
     * Обработка подтверждения успешного зачисления средств.
     * Обновляет статус перевода на COMPLETED и публикация TRANSFER_COMPLETED.
     * Финальный шаг в успешном сценарии Saga.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода
     * @param creditTransactionId идентификатор транзакции зачисления
     */
    @Transactional
    public void processCreditConfirmed(String eventId, UUID correlationId,
                                       UUID transferId, String creditTransactionId) {
        log.info("Processing CREDIT_CONFIRMED: eventId={}, transferId={}, correlationId={}, creditTransactionId={}",
                eventId, transferId, correlationId, creditTransactionId);

        // Проверка идемпотентности
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }

        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на COMPLETED
        transfer.processCreditConfirmed(creditTransactionId);
        transferRepository.save(transfer);

        // Сохраняем факт обработки события
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.CREDIT_CONFIRMED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие TRANSFER_COMPLETED в топик transfer.status
        outboxEventService.createTransferCompletedEvent(
                transferId.toString(),
                correlationId.toString()
        );

        log.info("Successfully processed CREDIT_CONFIRMED and published TRANSFER_COMPLETED: transferId={}", transferId);
    }

    /**
     * Обработка ошибки зачисления средств.
     * Обновляет статус перевода на COMPENSATING и публикует COMPENSATE_DEBIT.
     * Начало компенсационного сценария Saga.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода
     * @param errorReason причина ошибки зачисления
     */
    @Transactional
    public void processCreditFailed(String eventId, UUID correlationId,
                                    UUID transferId, String errorReason) {
        log.info("Processing CREDIT_FAILED: eventId={}, transferId={}, correlationId={}, errorReason={}",
                eventId, transferId, correlationId, errorReason);

        // Проверка идемпотентности
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }

        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на COMPENSATING
        transfer.processCreditFailed(errorReason);
        transferRepository.save(transfer);

        // Сохраняем факт обработки события
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.CREDIT_FAILED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие COMPENSATE_DEBIT в топик account.compensate.debit
        outboxEventService.createCompensateDebitEvent(
                transferId.toString(),
                correlationId.toString(),
                transfer.getFromAccount(),
                transfer.getAmount().getAmount(),
                transfer.getAmount().getCurrencyCode()
        );

        log.info("Successfully processed CREDIT_FAILED and published COMPENSATE_DEBIT: transferId={}", transferId);
    }

    /**
     * Обработка подтверждения успешной компенсации.
     * Обновляет статус перевода на COMPENSATED и публикует TRANSFER_COMPENSATED.
     * Финальный шаг в компенсационном сценарии Saga.
     *
     * @param eventId уникальный идентификатор события
     * @param correlationId идентификатор корреляции для трассировки
     * @param transferId идентификатор перевода
     */
    @Transactional
    public void processCompensateConfirmed(String eventId, UUID correlationId,
                                           UUID transferId) {
        log.info("Processing COMPENSATE_CONFIRMED: eventId={}, transferId={}, correlationId={}",
                eventId, transferId, correlationId);

        // Проверка идемпотентности
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event already processed: eventId={}", eventId);
            return;
        }

        Transfer transfer = transferRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transfer not found: " + transferId));

        // Обновляем статус перевода на COMPENSATED
        transfer.processCompensateConfirmed();
        transferRepository.save(transfer);

        // Сохраняем факт обработки события
        ProcessedEvent processedEvent = ProcessedEvent.newOf(
                eventId, transferId, ProcessedEventType.COMPENSATE_CONFIRMED);
        processedEventRepository.save(processedEvent);

        // Публикуем событие TRANSFER_COMPENSATED в топик transfer.status
        outboxEventService.createTransferCompensatedEvent(
                transferId.toString(),
                correlationId.toString()
        );

        log.info("Successfully processed COMPENSATE_CONFIRMED and published TRANSFER_COMPENSATED: transferId={}", transferId);
    }
}