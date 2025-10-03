package com.bankx.transfer.infrastructure.scheduler;

public class OutboxEventPublisher {
}

//todo: OutboxEventPublisher нужно будет создавать после :
// 0. созданы OutboxEventEntity и Repository
// 1. Работающего KafkaTemplate (Настройки кафка продюсера)
// 2. Реализованного EventPublisherPort через Outbox
// 3. Только после этого создать OutboxEventPublisher
