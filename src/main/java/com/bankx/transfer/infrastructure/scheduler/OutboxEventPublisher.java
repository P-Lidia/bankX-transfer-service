package com.bankx.transfer.infrastructure.scheduler;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.domain.repository.OutboxEventRepository;
import com.bankx.transfer.infrastructure.kafka.KafkaEventPublisher;
import com.bankx.transfer.shared.util.JsonConverter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

/**
 * Фоновый процесс для публикации событий из outbox таблицы в Kafka.
 * Реализует паттерн Outbox для гарантированной доставки событий.
 * Отвечает за:
 * - Поиск необработанных событий (статус NEW или FAILED с допустимым количеством ретраев)
 * - Публикацию событий в Kafka через KafkaEventPublisher
 * - Обновление статуса событий после успешной/неуспешной публикации
 * - Поддержку retry логики при ошибках отправки
 *
 * Соответствует требованиям ТЗ по надежной доставке событий (5.1).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final JsonConverter jsonConverter;

    @Value("${outbox.publisher.max-retries:3}")
    private int maxRetries;

    @Value("${outbox.publisher.batch-size:100}")
    private int batchSize;

    /**
     * Фоновый процесс, запускаемый по расписанию.
     * Ищет необработанные события и публикует их в Kafka.
     * Работает с фиксированным интервалом для минимизации нагрузки на БД.
     *
     * @implNote Метод транзакционный - гарантирует атомарность обновления статуса
     * событий и защиты от двойной обработки при конкурентном доступе.
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.poll-interval:5000}")
    @Transactional
    public void processOutboxEvents() {
        try {
            log.debug("Starting outbox event processing");

            // 1. Находим события, требующие обработки
            List<OutboxEvent> events = outboxEventRepository.findEventsForProcessing(maxRetries, batchSize);

            if (events.isEmpty()) {
                log.debug("No outbox events to process");
                return;
            }

            log.info("Found {} outbox events to process", events.size());

            // 2. Обрабатываем каждое событие
            for (OutboxEvent event : events) {
                try {
                    processSingleEvent(event);
                } catch (Exception e) {
                    log.error("Failed to process outbox event: id={}, eventType={}",
                            event.getId(), event.getEventType(), e);
                    // Продолжаем обработку других событий при ошибке
                }
            }

            log.info("Completed processing {} outbox events", events.size());

        } catch (Exception e) {
            log.error("Error in outbox event processing scheduler", e);
        }
    }

    /**
     * Обрабатывает одно outbox событие:
     * 1. Обновляет статус на PROCESSING
     * 2. Публикует в Kafka через KafkaEventPublisher
     * 3. Обновляет статус на SENT при успехе или FAILED при ошибке
     *
     * @param event событие для обработки
     */
    private void processSingleEvent(OutboxEvent event) {
        try {
            // 1. Обновляем статус на PROCESSING
            OutboxEvent processingEvent = event.markAsProcessing();
            outboxEventRepository.save(processingEvent);

            log.info("Processing outbox event: id={}, eventType={}, correlationId={}",
                    event.getId(), event.getEventType(), event.getCorrelationId());

            // 2. Создаем KafkaEvent из OutboxEvent
            KafkaEvent kafkaEvent = createKafkaEvent(event);

            // 3. Публикуем в Kafka
            kafkaEventPublisher.publishEventDirectly(kafkaEvent);

            // 4. Обновляем статус на SENT
            OutboxEvent sentEvent = event.markAsSent();
            outboxEventRepository.save(sentEvent);

            log.info("Successfully published outbox event to Kafka: id={}, eventType={}",
                    event.getId(), event.getEventType());

        } catch (Exception e) {
            log.error("Failed to publish outbox event to Kafka: id={}, eventType={}",
                    event.getId(), event.getEventType(), e);

            // Обновляем статус на FAILED с ошибкой
            OutboxEvent failedEvent = event.markAsFailed(e.getMessage());
            outboxEventRepository.save(failedEvent);

            throw new RuntimeException("Failed to publish event to Kafka", e);
        }
    }

    /**
     * Создает KafkaEvent из OutboxEvent.
     * Десериализует payload из JSON строки в объект для отправки в Kafka.
     * Сохраняет correlationId как UUID для единообразия.
     *
     * @param outboxEvent событие из outbox таблицы
     * @return KafkaEvent готовый для отправки в Kafka
     */
    private KafkaEvent createKafkaEvent(OutboxEvent outboxEvent) {
        // Десериализуем payload из JSON строки
        Object payload = jsonConverter.fromJson(outboxEvent.getPayload(), Object.class);

        return KafkaEvent.builder()
                .eventId("evt_" + UUID.randomUUID())
                .eventType(outboxEvent.getEventType())
                .correlationId(outboxEvent.getCorrelationId())
                .transferId(outboxEvent.getAggregateId())
                .payload(payload)
                .timestamp(outboxEvent.getCreatedAt().atZone(ZoneId.systemDefault()).toInstant())
                .build();
    }

    /**
     * Метод для ручного запуска обработки событий.
     * Может использоваться для отладки или принудительной обработки.
     */
    @Transactional
    public void triggerManualProcessing() {
        log.info("Manual trigger of outbox event processing");
        processOutboxEvents();
    }
}