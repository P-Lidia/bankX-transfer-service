package com.bankx.transfer.infrastructure.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация Kafka топиков для Transfer Service.
 * Определяет названия топиков, используемых для обмена сообщениями между микросервисами.
 * Соответствует требованиям ТЗ по схеме взаимодействия (2.2).
 *
 * Все топики настраиваются через application.yml для обеспечения гибкости.
 * При изменении названий топиков в Kafka не требуется перекомпиляция кода.
 */
@Configuration
@ConfigurationProperties(prefix = "kafka.topics")
@Getter
@Setter
public class KafkaTopicConfig {

    /**
     * Топик для команд создания переводов.
     * Входящий топик: Payment Orchestrator → Transfer Service.
     * Содержит команды TRANSFER_COMMAND.
     * Соответствует требованию ТЗ 2.1.
     */
    private String transferCommand;

    /**
     * Топик для запросов на списание средств.
     * Исходящий топик: Transfer Service → Account Service.
     * Содержит события DEBIT_REQUEST.
     * Соответствует требованию ТЗ 2.2.2.
     */
    private String accountDebitRequest;

    /**
     * Топик для запросов на зачисление средств.
     * Исходящий топик: Transfer Service → Account Service.
     * Содержит события CREDIT_REQUEST.
     * Соответствует требованию ТЗ 2.2.2.
     */
    private String accountCreditRequest;

    /**
     * Топик для компенсационных списаний.
     * Исходящий топик: Transfer Service → Account Service.
     * Содержит события COMPENSATE_DEBIT.
     * Соответствует требованию ТЗ 2.2.2.
     */
    private String accountCompensateDebit;

    /**
     * Топик для ответов на списание средств.
     * Входящий топик: Account Service → Transfer Service.
     * Содержит события DEBIT_CONFIRMED и DEBIT_FAILED.
     * Соответствует требованию ТЗ 2.2.1.
     */
    private String accountDebitResponse;

    /**
     * Топик для ответов на зачисление средств.
     * Входящий топик: Account Service → Transfer Service.
     * Содержит события CREDIT_CONFIRMED и CREDIT_FAILED.
     * Соответствует требованию ТЗ 2.2.1.
     */
    private String accountCreditResponse;

    /**
     * Топик для статусов переводов.
     * Исходящий топик: Transfer Service → Payment Orchestrator.
     * Содержит события TRANSFER_COMPLETED, TRANSFER_FAILED, TRANSFER_COMPENSATED.
     * Соответствует требованию ТЗ 2.1.
     */
    private String transferStatus;

    // ========== ГЕТТЕРЫ ДЛЯ УДОБСТВА ИСПОЛЬЗОВАНИЯ ==========

    /**
     * Получает название топика для команд перевода.
     * Используется KafkaConsumer для подписки на команды.
     *
     * @return название топика transfer.command
     */
    public String getTransferCommandTopic() {
        return transferCommand;
    }

    /**
     * Получает название топика для запросов на списание.
     * Используется KafkaProducer для отправки DEBIT_REQUEST.
     *
     * @return название топика account.debit.request
     */
    public String getAccountDebitRequestTopic() {
        return accountDebitRequest;
    }

    /**
     * Получает название топика для запросов на зачисление.
     * Используется KafkaProducer для отправки CREDIT_REQUEST.
     *
     * @return название топика account.credit.request
     */
    public String getAccountCreditRequestTopic() {
        return accountCreditRequest;
    }

    /**
     * Получает название топика для компенсационных списаний.
     * Используется KafkaProducer для отправки COMPENSATE_DEBIT.
     *
     * @return название топика account.compensate.debit
     */
    public String getAccountCompensateDebitTopic() {
        return accountCompensateDebit;
    }

    /**
     * Получает название топика для ответов на списание.
     * Используется KafkaConsumer для подписки на ответы от Account Service.
     *
     * @return название топика account.debit.response
     */
    public String getAccountDebitResponseTopic() {
        return accountDebitResponse;
    }

    /**
     * Получает название топика для ответов на зачисление.
     * Используется KafkaConsumer для подписки на ответы от Account Service.
     *
     * @return название топика account.credit.response
     */
    public String getAccountCreditResponseTopic() {
        return accountCreditResponse;
    }

    /**
     * Получает название топика для статусов переводов.
     * Используется KafkaProducer для публикации финальных статусов.
     *
     * @return название топика transfer.status
     */
    public String getTransferStatusTopic() {
        return transferStatus;
    }

    /**
     * Проверяет, является ли топик входящим для Transfer Service.
     * Входящие топики: transfer.command, account.debit.response, account.credit.response.
     *
     * @param topic название топика для проверки
     * @return true если топик входящий, false если исходящий
     */
    public boolean isIncomingTopic(String topic) {
        if (topic == null) {
            return false;
        }
        return topic.equals(transferCommand) ||
                topic.equals(accountDebitResponse) ||
                topic.equals(accountCreditResponse);
    }

    /**
     * Проверяет, является ли топик исходящим из Transfer Service.
     * Исходящие топики: account.debit.request, account.credit.request,
     * account.compensate.debit, transfer.status.
     *
     * @param topic название топика для проверки
     * @return true если топик исходящий, false если входящий
     */
    public boolean isOutgoingTopic(String topic) {
        if (topic == null) {
            return false;
        }
        return topic.equals(accountDebitRequest) ||
                topic.equals(accountCreditRequest) ||
                topic.equals(accountCompensateDebit) ||
                topic.equals(transferStatus);
    }

    /**
     * Определяет тип события на основе топика.
     * Используется для логирования и мониторинга.
     *
     * @param topic название топика
     * @return тип события (INCOMING, OUTGOING) или UNKNOWN
     */
    public String getTopicType(String topic) {
        if (isIncomingTopic(topic)) {
            return "INCOMING";
        } else if (isOutgoingTopic(topic)) {
            return "OUTGOING";
        } else {
            return "UNKNOWN";
        }
    }

    /**
     * Получает человекочитаемое описание топика.
     * Используется для логирования и административных интерфейсов.
     *
     * @param topic название топика
     * @return описание топика или "Неизвестный топик"
     */
    public String getTopicDescription(String topic) {
        if (topic == null) {
            return "Неизвестный топик (null)";
        }

        // Используем обычный if-else вместо pattern matching
        if (topic.equals(transferCommand)) {
            return "Топик команд перевода (Payment Orchestrator → Transfer Service)";
        } else if (topic.equals(accountDebitRequest)) {
            return "Топик запросов списания (Transfer Service → Account Service)";
        } else if (topic.equals(accountCreditRequest)) {
            return "Топик запросов зачисления (Transfer Service → Account Service)";
        } else if (topic.equals(accountCompensateDebit)) {
            return "Топик компенсационных списаний (Transfer Service → Account Service)";
        } else if (topic.equals(accountDebitResponse)) {
            return "Топик ответов на списание (Account Service → Transfer Service)";
        } else if (topic.equals(accountCreditResponse)) {
            return "Топик ответов на зачисление (Account Service → Transfer Service)";
        } else if (topic.equals(transferStatus)) {
            return "Топик статусов переводов (Transfer Service → Payment Orchestrator)";
        } else {
            return "Неизвестный топик: " + topic;
        }
    }

    @Override
    public String toString() {
        return "KafkaTopicConfig{" +
                "transferCommand='" + transferCommand + '\'' +
                ", accountDebitRequest='" + accountDebitRequest + '\'' +
                ", accountCreditRequest='" + accountCreditRequest + '\'' +
                ", accountCompensateDebit='" + accountCompensateDebit + '\'' +
                ", accountDebitResponse='" + accountDebitResponse + '\'' +
                ", accountCreditResponse='" + accountCreditResponse + '\'' +
                ", transferStatus='" + transferStatus + '\'' +
                '}';
    }
}