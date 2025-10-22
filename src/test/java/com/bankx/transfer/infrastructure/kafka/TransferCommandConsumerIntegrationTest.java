package com.bankx.transfer.infrastructure.kafka;

import com.bankx.transfer.application.service.TransferCommandService;
import com.bankx.transfer.infrastructure.kafka.dto.TransferCommandMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Testcontainers
class TransferCommandConsumerIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("transfer_test")
            .withUsername("test")
            .withPassword("test");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Database properties
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Для ускорения тестов (опционально)
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.properties.hibernate.dialect", () -> "org.hibernate.dialect.PostgreSQLDialect");

        // Kafka properties
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "test-group-" + UUID.randomUUID()); // уникальная группа
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockBean
    private TransferCommandService transferCommandService;

    @Test
    void shouldConsumeTransferCommand() {
        // Given
        String correlationId = UUID.randomUUID().toString();
        TransferCommandMessage message = createTestMessage(correlationId);

        // When
        kafkaTemplate.send("transfer.command", correlationId, message);

        // Then
        verify(transferCommandService, timeout(15000))
                .createTransfer(
                        eq(correlationId),    // используем eq() для конкретного значения
                        any(),
                        any(),
                        any(),
                        any()
                );
    }

    private TransferCommandMessage createTestMessage(String correlationId) {
        TransferCommandMessage message = new TransferCommandMessage();
        message.setEventId("evt_" + UUID.randomUUID());
        message.setEventType("TRANSFER_COMMAND");
        message.setTimestamp(Instant.now());
        message.setCorrelationId(correlationId);

        TransferCommandMessage.Payload payload = new TransferCommandMessage.Payload();
        payload.setFromAccount("ACC001");
        payload.setToAccount("ACC002");
        payload.setAmount(new BigDecimal("100.50"));
        payload.setCurrency("USD");
        payload.setDescription("Test transfer");

        message.setPayload(payload);
        return message;
    }
}