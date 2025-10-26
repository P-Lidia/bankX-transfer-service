package com.bankx.transfer.application.port;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Порт для публикации событий в Kafka.
 * Определяет контракты для отправки событий во внешние системы.
 */
public interface EventPublisherPort {

    /**
     * Публикует запрос на списание средств
     */
    void publishDebitRequest(String transferId, UUID correlationId,
                             String accountId, BigDecimal amount, String currency);

    /**
     * Публикует запрос на зачисление средств
     */
    void publishCreditRequest(String transferId, UUID correlationId,
                              String accountId, BigDecimal amount, String currency);

    /**
     * Публикует запрос на компенсацию списания
     */
    void publishCompensateDebit(String transferId, UUID correlationId,
                                String accountId, BigDecimal amount, String currency);

    /**
     * Публикует статус перевода
     */
    void publishTransferStatus(String transferId, UUID correlationId,
                               String status, String reason);
}