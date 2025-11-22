package com.bankx.transfer.infrastructure.config;

import com.bankx.transfer.application.dto.KafkaEvent;
import com.bankx.transfer.infrastructure.kafka.dto.AccountEventMessage;
import com.bankx.transfer.infrastructure.kafka.dto.TransferCommandMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

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

    // НОВЫЙ БИН: Producer для Object (для KafkaEventPublisher) - ФИКС ОШИБКИ
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // НОВЫЙ БИН: KafkaTemplate для Object (для KafkaEventPublisher) - ФИКС ОШИБКИ
    @Bean
    public KafkaTemplate<String, Object> kafkaObjectTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // Producer для KafkaEvent (для OutboxEventPublisher)
    @Bean
    public ProducerFactory<String, KafkaEvent> kafkaEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, KafkaEvent> kafkaTemplate() {
        return new KafkaTemplate<>(kafkaEventProducerFactory());
    }

    // Consumer для TransferCommandMessage (для TransferCommandConsumer)
    @Bean
    public ConsumerFactory<String, TransferCommandMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TransferCommandMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.bankx.transfer.*");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransferCommandMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TransferCommandMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    // Consumer для AccountEventMessage (для AccountEventConsumer) - ИСПРАВЛЕННАЯ ВЕРСИЯ
    @Bean
    public ConsumerFactory<String, AccountEventMessage> accountEventConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AccountEventMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.bankx.transfer.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "AccountEventMessage:com.bankx.transfer.infrastructure.kafka.dto.AccountEventMessage");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AccountEventMessage> accountEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AccountEventMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(accountEventConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Добавляем обработку ошибок (упрощенная версия без сложных зависимостей)
        factory.setCommonErrorHandler(new CommonErrorHandler() {
            public void handleOtherException(Exception thrownException, Object consumer, Object container, boolean batchListener) {
                log.error("Error in Kafka listener: {}", thrownException.getMessage(), thrownException);
            }
        });

        return factory;
    }
}