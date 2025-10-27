package com.bankx.transfer.application.service;

import com.bankx.transfer.domain.model.OutboxEvent;
import com.bankx.transfer.domain.repository.OutboxEventRepository;
import com.bankx.transfer.shared.util.JsonConverter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonConverter jsonConverter;

    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload,
                                  UUID correlationId) {
        try {
            log.info("🟡 Creating outbox event: type={}, aggregateId={}, correlationId={}",
                    eventType, aggregateId, correlationId);

            // ВАЖНО: Проверяем, что correlationId не null
            if (correlationId == null) {
                log.warn("🔴 CorrelationId is null, generating new one");
                correlationId = UUID.randomUUID();
            }

            String payloadJson = jsonConverter.toJson(payload);
            log.debug("Payload JSON: {}", payloadJson);

            // Убедимся, что все обязательные поля установлены
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .payload(payloadJson)
                    .correlationId(correlationId) // Гарантируем, что correlationId установлен
                    .build();

            OutboxEvent savedEvent = outboxEventRepository.save(outboxEvent);
            log.info("🟢 Outbox event saved successfully: id={}, correlationId={}",
                    savedEvent.getId(), savedEvent.getCorrelationId());

        } catch (Exception e) {
            log.error("🔴 Failed to create outbox event: type={}, aggregateId={}, correlationId={}",
                    eventType, aggregateId, correlationId, e);
            throw new RuntimeException("Failed to create outbox event", e);
        }
    }

    @Transactional
    public void createOutboxEvent(String aggregateType, UUID aggregateId,
                                  String eventType, Map<String, Object> payload) {
        // Всегда генерируем correlationId если не предоставлен
        UUID generatedCorrelationId = UUID.randomUUID();
        createOutboxEvent(aggregateType, aggregateId, eventType, payload, generatedCorrelationId);
    }
}