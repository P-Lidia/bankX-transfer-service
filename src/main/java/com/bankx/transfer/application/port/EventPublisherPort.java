package com.bankx.transfer.application.port;

import java.math.BigDecimal;

/**
 * Порт для публикации событий в Kafka.
 * Определяет контракты для отправки событий во внешние системы.
 */
public interface EventPublisherPort {

    /**
     * Публикует запрос на списание средств
     */
    void publishDebitRequest(String transferId, String correlationId,
                             String accountId, BigDecimal amount, String currency);

    /**
     * Публикует запрос на зачисление средств
     */
    void publishCreditRequest(String transferId, String correlationId,
                              String accountId, BigDecimal amount, String currency);

    /**
     * Публикует запрос на компенсацию списания
     */
    void publishCompensateDebit(String transferId, String correlationId,
                                String accountId, BigDecimal amount, String currency);

    /**
     * Публикует статус перевода
     */
    void publishTransferStatus(String transferId, String correlationId,
                               String status, String reason);
}