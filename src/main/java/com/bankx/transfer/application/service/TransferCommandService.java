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

/**
 * Сервис для обработки команд создания переводов в рамках CQRS.
 * Отвечает за:
 * - Создание новых переводов с гарантией идемпотентности
 * - Инициализацию Saga процесса через Outbox Pattern
 * - Координацию первой фазы двухфазного коммита (списание средств)
 * - Сохранение перевода и outbox события в одной транзакции
 *
 * Является частью бизнес-логики Transfer Service и реализует
 * сценарий "Инициирование перевода" согласно техническому заданию.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransferCommandService {
    private final TransferRepository transferRepository;
    private final OutboxEventService outboxEventService;

    /**
     * Создает новый перевод и инициирует процесс списания средств через Outbox Pattern.
     * Гарантирует идемпотентность через проверку correlationId и атомарность
     * через транзакционное сохранение перевода и outbox события.
     *
     * @param correlationId уникальный идентификатор корреляции для трассировки и идемпотентности
     * @param fromAccount номер счета отправителя (Value Object)
     * @param toAccount номер счета получателя (Value Object)
     * @param amount денежная сумма перевода (Value Object)
     * @param description описание перевода для аудита
     *
     * @implNote Метод выполняется в транзакции, обеспечивая атомарность:
     * 1. Проверка идемпотентности по correlationId
     * 2. Сохранение перевода в статусе PENDING
     * 3. Создание outbox события DEBIT_REQUEST для инициации Saga
     * При ошибке на любом этапе все изменения откатываются
     */
    @Transactional
    public void createTransfer(UUID correlationId,
                               AccountNumber fromAccount,
                               AccountNumber toAccount,
                               Money amount,
                               String description) {
        log.info("Creating transfer: correlationId={}, from={}, to={}, amount={}",
                correlationId, fromAccount.value(), toAccount.value(), amount.toDisplayString());

        // Проверяем, не существует ли уже перевод с таким correlationId (идемпотентность)
        // Защита от дублирования команд в распределенной системе
        if (transferRepository.existsByCorrelationId(correlationId)) {
            log.warn("Transfer with correlationId {} already exists", correlationId);
            return; // Идемпотентность - игнорируем дубликат
        }

        // Создаем доменный объект перевода
        // Transfer агрегат инкапсулирует бизнес-правила создания перевода
        Transfer transfer = new Transfer(correlationId, fromAccount, toAccount, amount, description);

        // Сохраняем перевод в состоянии PENDING
        // База данных выступает как система сохранности состояния
        Transfer savedTransfer = transferRepository.save(transfer);
        log.info("Transfer created successfully: id={}, correlationId={}",
                savedTransfer.getId(), correlationId);

        // Публикуем событие DEBIT_REQUEST для списание средств через Outbox Pattern
        // Вместо прямой отправки в Kafka сохраняем в outbox таблицу
        // Это гарантирует, что событие не потеряется даже при недоступности Kafka
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

        // На этом этапе Saga процесса инициирована:
        // 1. Перевод сохранен в состоянии PENDING
        // 2. Событие DEBIT_REQUEST сохранено в outbox_events
        // 3. OutboxEventPublisher асинхронно отправит событие в Kafka
        // 4. Account Service получит событие и выполнит списание
    }
}