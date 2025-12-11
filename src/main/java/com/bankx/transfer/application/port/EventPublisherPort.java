package com.bankx.transfer.application.port;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Порт для публикации событий во внешние системы (Kafka).
 * Определяет контракт для отправки событий, связанных с операциями перевода.
 * Реализует принципы Hexagonal Architecture и Outbox Pattern.
 * Отвечает за координацию Saga процесса через события.
 */
public interface EventPublisherPort {

    /**
     * Публикует событие запроса на списание средств.
     * Используется для инициации первой фазы Saga (списание).
     * Соответствует событию DEBIT_REQUEST в схеме Saga.
     *
     * @param transferId идентификатор перевода
     * @param correlationId идентификатор корреляции для трассировки
     * @param accountId счет для списания
     * @param amount сумма списания
     * @param currency валюта операции
     */
    void publishDebitRequest(String transferId, UUID correlationId,
                             String accountId, BigDecimal amount, String currency);

    /**
     * Публикует событие запроса на зачисление средств.
     * Используется для второй фазы Saga (зачисление).
     * Соответствует событию CREDIT_REQUEST в схеме Saga.
     *
     * @param transferId идентификатор перевода
     * @param correlationId идентификатор корреляции для трассировки
     * @param accountId счет для зачисления
     * @param amount сумма зачисления
     * @param currency валюта операции
     */
    void publishCreditRequest(String transferId, UUID correlationId,
                              String accountId, BigDecimal amount, String currency);

    /**
     * Публикует событие компенсационного списания.
     * Используется для фазы компенсации при ошибке зачисления.
     * Соответствует событию COMPENSATE_DEBIT в схеме Saga.
     *
     * @param transferId идентификатор перевода
     * @param correlationId идентификатор корреляции для трассировки
     * @param accountId счет для возврата средств
     * @param amount сумма возврата
     * @param currency валюта операции
     */
    void publishCompensateDebit(String transferId, UUID correlationId,
                                String accountId, BigDecimal amount, String currency);

    /**
     * Публикует событие изменения статуса перевода.
     * Используется для уведомления о финальном статусе Saga.
     * Соответствует событиям TRANSFER_COMPLETED/FAILED/COMPENSATED.
     *
     * @param transferId идентификатор перевода
     * @param correlationId идентификатор корреляции для трассировки
     * @param status финальный статус перевода
     * @param reason причина статуса (опционально)
     */
    void publishTransferStatus(String transferId, UUID correlationId,
                               String status, String reason);
}