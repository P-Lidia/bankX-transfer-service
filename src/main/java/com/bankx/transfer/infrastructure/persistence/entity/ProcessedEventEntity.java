package com.bankx.transfer.infrastructure.persistence.entity;

import com.bankx.transfer.domain.model.ProcessedEventType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * JPA сущность для хранения информации об обработанных событиях в PostgreSQL.
 * Реализует механизм идемпотентности для Kafka-консьюмеров.
 * Каждое событие, обработанное сервисом, регистрируется в этой таблице
 * для предотвращения повторной обработки.
 */
@Entity
@Table(name = "processed_events", indexes = {
        @Index(name = "idx_processed_events_event_id", columnList = "event_id", unique = true),
        @Index(name = "idx_processed_events_correlation_type", columnList = "correlation_id, event_type", unique = true),
        @Index(name = "idx_processed_events_processed_at", columnList = "processed_at"),
        @Index(name = "idx_processed_events_created_at", columnList = "created_at"),
        @Index(name = "idx_processed_events_transfer_id", columnList = "transfer_id") // Новый индекс
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class ProcessedEventEntity {

    /**
     * Уникальный идентификатор записи.
     * Генерируется автоматически при сохранении.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", columnDefinition = "UUID")
    @EqualsAndHashCode.Include
    private UUID id;

    /**
     * Уникальный идентификатор события из Kafka.
     * Используется для прямой проверки идемпотентности.
     */
    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    /**
     * Идентификатор корреляции события из Kafka.
     * Используется в сочетании с eventType для составного ключа идемпотентности.
     */
    @Column(name = "correlation_id", nullable = false)
    private String correlationId;

    /**
     * Тип обработанного события.
     * Определяет категорию события и его обработчик.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 50)
    private ProcessedEventType eventType;

    /**
     * Дата и время обработки события.
     * Устанавливается в момент сохранения записи в БД.
     */
    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    /**
     * Полезная нагрузка события в формате JSON.
     * Содержит исходные данные события для отладки и аудита.
     */
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    /**
     * Идентификатор перевода, связанного с событием.
     * Опциональное поле, присутствует только для событий, связанных с переводами.
     */
    @Column(name = "transfer_id", columnDefinition = "UUID")
    private UUID transferId;

    /**
     * Дата и время создания записи.
     * Устанавливается автоматически при первом сохранении.
     */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}