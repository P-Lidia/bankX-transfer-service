package com.bankx.transfer.infrastructure.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация имен топиков Kafka.
 * Значения могут быть переопределены в application.yml
 */
@Getter
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topics.transfer.command:transfer.command}")
    private String transferCommandTopic;

    @Value("${kafka.topics.transfer.status:transfer.status}")
    private String transferStatusTopic;

    @Value("${kafka.topics.account.debit.request:account.debit.request}")
    private String accountDebitRequestTopic;

    @Value("${kafka.topics.account.debit.response:account.debit.response}")
    private String accountDebitResponseTopic;

    @Value("${kafka.topics.account.credit.request:account.credit.request}")
    private String accountCreditRequestTopic;

    @Value("${kafka.topics.account.credit.response:account.credit.response}")
    private String accountCreditResponseTopic;

    @Value("${kafka.topics.account.compensate.debit:account.compensate.debit}")
    private String accountCompensateDebitTopic;

    @Value("${kafka.topics.dlt:dead-letter-queue}")
    private String deadLetterTopic;
}