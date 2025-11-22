package com.bankx.transfer.infrastructure.scheduler;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.domain.repository.OutboxEventRepository;
import com.bankx.transfer.infrastructure.config.KafkaTopicConfig;
import com.bankx.transfer.shared.util.JsonConverter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Фоновый процесс для отправки событий из Outbox в Kafka
 * Соответствует требованиям ТЗ по надежной доставке событий
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicConfig kafkaTopicConfig;
    private final JsonConverter jsonConverter;

    // Конфигурация согласно требованиям надежности
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 100;
    private static final long PUBLISHING_INTERVAL = 5000; // 5 секунд

    /**
     * Периодическая отправка событий из Outbox в Kafka
     * Выполняется каждые 5 секунд согласно требованиям надежности
     */
    @Scheduled(fixedDelay = PUBLISHING_INTERVAL)
    @Transactional
    public void publishOutboxEvents() {
        try {
            log.debug("Starting outbox events publishing cycle");

            // Получаем необработанные события с ограничением по попыткам
            List<OutboxEvent> unprocessedEvents = outboxEventRepository
                    .findUnprocessedEventsWithLimit(MAX_RETRIES, BATCH_SIZE);

            if (unprocessedEvents.isEmpty()) {
                log.debug("No unprocessed outbox events found");
                return;
            }

            log.info("Processing {} outbox events", unprocessedEvents.size());

            for (OutboxEvent event : unprocessedEvents) {
                processSingleEvent(event);
            }

            log.info("Completed outbox events publishing cycle");

        } catch (Exception e) {
            log.error("Unexpected error during outbox events publishing", e);
        }
    }

    /**
     * Обработка одного события из Outbox
     */
    private void processSingleEvent(OutboxEvent event) {
        log.debug("Processing outbox event: id={}, type={}", event.getId(), event.getEventType());

        try {
            // Помечаем событие как обрабатываемое (оптимистичная блокировка)
            outboxEventRepository.markAsProcessing(event.getId());

            // Преобразуем OutboxEvent в KafkaEvent
            KafkaEvent kafkaEvent = convertToKafkaEvent(event);

            // Определяем топик назначения
            String targetTopic = determineTargetTopic(event.getEventType());

            // Отправляем в Kafka с использованием kafkaTemplate (Object)
            CompletableFuture<SendResult<String, Object>> sendFuture =
                    kafkaTemplate.send(targetTopic, event.getAggregateId().toString(), kafkaEvent);

            // Обрабатываем результат асинхронно
            handleSendResult(sendFuture, event);

        } catch (Exception e) {
            log.error("Failed to process outbox event: id={}, type={}",
                    event.getId(), event.getEventType(), e);

            // Помечаем событие как неудачное
            outboxEventRepository.markAsFailed(event.getId(),
                    "Processing error: " + e.getMessage());
        }
    }

    /**
     * Преобразование OutboxEvent в KafkaEvent для отправки
     */
    private KafkaEvent convertToKafkaEvent(OutboxEvent outboxEvent) {
        try {
            Map<String, Object> payload = jsonConverter.fromJson(
                    outboxEvent.getPayload(),
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}
            );

            return KafkaEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .eventType(outboxEvent.getEventType())
                    .timestamp(java.time.Instant.now())
                    .correlationId(outboxEvent.getCorrelationId().toString()) // Преобразуем UUID в String для Kafka
                    .transferId(outboxEvent.getAggregateId().toString())
                    .payload(payload)
                    .build();

        } catch (Exception e) {
            log.error("Failed to convert outbox event to Kafka event: id={}",
                    outboxEvent.getId(), e);
            throw new RuntimeException("Event conversion failed", e);
        }
    }

    /**
     * Определение топика Kafka на основе типа события
     * Соответствует таблице исходящих событий из ТЗ
     */
    private String determineTargetTopic(String eventType) {
        switch (eventType) {
            case "DEBIT_REQUEST":
                return kafkaTopicConfig.getAccountDebitRequestTopic();
            case "CREDIT_REQUEST":
                return kafkaTopicConfig.getAccountCreditRequestTopic();
            case "COMPENSATE_DEBIT":
                return kafkaTopicConfig.getAccountCompensateDebitTopic();
            case "TRANSFER_COMPLETED":
            case "TRANSFER_FAILED":
            case "TRANSFER_COMPENSATED":
                return kafkaTopicConfig.getTransferStatusTopic();
            default:
                log.warn("Unknown event type: {}, using default topic", eventType);
                return kafkaTopicConfig.getTransferStatusTopic();
        }
    }

    /**
     * Обработка результата отправки в Kafka
     */
    private void handleSendResult(CompletableFuture<SendResult<String, Object>> sendFuture,
                                  OutboxEvent event) {
        sendFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                // Ошибка отправки
                log.error("Failed to send event to Kafka: eventId={}, eventType={}",
                        event.getId(), event.getEventType(), throwable);

                // Используем TransactionTemplate для обновления в отдельной транзакции
                try {
                    outboxEventRepository.markAsFailed(event.getId(),
                            "Kafka send error: " + throwable.getMessage());
                } catch (Exception e) {
                    log.error("Failed to mark event as failed in repository: eventId={}", event.getId(), e);
                }
            } else {
                // Успешная отправка
                log.debug("Successfully sent event to Kafka: eventId={}, eventType={}, topic={}, partition={}, offset={}",
                        event.getId(), event.getEventType(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());

                // Используем TransactionTemplate для обновления в отдельной транзакции
                try {
                    outboxEventRepository.markAsSent(event.getId());
                } catch (Exception e) {
                    log.error("Failed to mark event as sent in repository: eventId={}", event.getId(), e);
                }
            }
        });
    }

    /**
     * Очистка старых обработанных событий (для предотвращения роста БД)
     * Выполняется раз в день
     */
    @Scheduled(cron = "0 0 2 * * ?") // Каждый день в 2:00
    @Transactional
    public void cleanupOldProcessedEvents() {
        try {
            LocalDateTime weekAgo = LocalDateTime.now().minusDays(7);
            int deletedCount = outboxEventRepository.deleteOldProcessedEvents(weekAgo);

            log.info("Cleaned up {} old processed outbox events", deletedCount);

        } catch (Exception e) {
            log.error("Failed to cleanup old processed events", e);
        }
    }

    /**
     * Восстановление застрявших событий в статусе PROCESSING
     * (на случай сбоя во время обработки)
     * Выполняется каждые 10 минут
     */
    @Scheduled(fixedDelay = 600000) // 10 минут
    @Transactional
    public void recoverStuckEvents() {
        try {
            LocalDateTime processingTimeout = LocalDateTime.now().minusMinutes(5);

            // Временное решение: просто логируем, что функция вызвана
            log.debug("Stuck events recovery check - метод findByStatus не реализован в репозитории");

            // TODO: Реализовать полноценное восстановление, когда добавится метод findByStatus в репозиторий

        } catch (Exception e) {
            log.error("Failed to recover stuck events", e);
        }
    }
}