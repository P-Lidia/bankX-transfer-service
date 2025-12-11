package com.bankx.transfer.infrastructure.config;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.infrastructure.kafka.dto.AccountEventMessage;
import com.bankx.transfer.infrastructure.kafka.dto.TransferCommandMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Минимальная конфигурация Kafka для работы с TransferCommandMessage и KafkaEvent
 */
@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:transfer-service-group}")
    private String consumerGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.producer.retries:3}")
    private String retries;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    // Producer Configuration для Object (общий)
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Базовые настройки
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Настройки надежности
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(retries));
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Настройки производительности
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    // Общий KafkaTemplate для Object
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());

        template.setProducerListener(new ProducerListener<String, Object>() {
            @Override
            public void onSuccess(ProducerRecord<String, Object> producerRecord, RecordMetadata recordMetadata) {
                log.info("Successfully sent Kafka event to topic: {} partition: {} offset: {}",
                        producerRecord.topic(), recordMetadata.partition(), recordMetadata.offset());
            }
        });

        return template;
    }

    // Consumer Configuration для TransferCommandMessage
    @Bean
    public ConsumerFactory<String, TransferCommandMessage> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Базовые настройки
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Настройки для обработки ошибок десериализации
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TransferCommandMessage.class.getName());
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.bankx.transfer.*");

        // Настройки надежности
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Настройки для идемпотентной обработки
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransferCommandMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TransferCommandMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    log.error("Message processing failed after all retries. Sending to DLT: topic={}, offset={}, key={}",
                            record.topic(), record.offset(), record.key(), exception);
                },
                new FixedBackOff(1000L, 3)
        );

        errorHandler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class,
                org.springframework.messaging.converter.MessageConversionException.class,
                IllegalArgumentException.class
        );
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}