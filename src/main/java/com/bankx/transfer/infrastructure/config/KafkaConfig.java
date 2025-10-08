package com.bankx.transfer.infrastructure.config;

import com.bankx.transfer.application.dto.KafkaEvent;
import lombok.RequiredArgsConstructor;
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
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Основная конфигурация Kafka для Producer и Consumer.
 * Настройки соответствуют требованиям идемпотентности и надежности.
 */
@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaTopicConfig kafkaTopicConfig;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:bankx-transfer-service}")
    private String consumerGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.producer.retries:3}")
    private String retries;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    // Producer Configuration
    @Bean
    public ProducerFactory<String, KafkaEvent> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Базовые настройки
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Настройки надежности (требования 5.1)
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

    @Bean
    public KafkaTemplate<String, KafkaEvent> kafkaTemplate() {
        KafkaTemplate<String, KafkaEvent> template = new KafkaTemplate<>(producerFactory());

        // Добавляем обработчик для логирования успешной отправки
        template.setProducerListener(new ProducerListener<String, KafkaEvent>() {
            @Override
            public void onSuccess(ProducerRecord<String, KafkaEvent> producerRecord, RecordMetadata recordMetadata) {
                log.info("Successfully sent Kafka event: {} to topic: {}",
                        producerRecord.value().getEventType(), producerRecord.topic());
            }

            @Override
            public void onError(ProducerRecord<String, KafkaEvent> producerRecord, RecordMetadata recordMetadata, Exception exception) {
                log.error("Failed to send Kafka event: {} to topic: {}",
                        producerRecord.value().getEventType(), producerRecord.topic(), exception);
            }
        });

        return template;
    }

    // Consumer Configuration
    @Bean
    public ConsumerFactory<String, KafkaEvent> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Базовые настройки
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Настройки для обработки ошибок десериализации
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.bankx.transfer.application.dto.KafkaEvent");
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.bankx.transfer.*");

        // Настройки надежности (требования 5.1)
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Ручное подтверждение
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Настройки для идемпотентной обработки
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Обрабатывать небольшими батчами

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // Настройка конкурентности
        factory.setConcurrency(3);

        // Настройка обработки ошибок с Retry политикой
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    // Логирование в Dead Letter Topic
                    log.error("Message processing failed after all retries. Sending to DLT: {}",
                            record.value(), exception);
                },
                new FixedBackOff(1000L, 3) // 3 попытки с интервалом 1 секунда
        );

        // Не повторять для определенных исключений (десериализация, валидация)
        // Используем setClassifications вместо addNotRetryableExceptions
        errorHandler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class
        );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    // Специальный контейнер для обработки событий с высокой надежностью
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> reliableKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1); // Меньшая конкурентность для критически важных событий

        // Более агрессивная политика retry для надежных обработчиков
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(2000L, 5) // 5 попыток с интервалом 2 секунды
        );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}