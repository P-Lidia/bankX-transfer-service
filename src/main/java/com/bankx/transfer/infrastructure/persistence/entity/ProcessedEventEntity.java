package com.bankx.transfer.infrastructure.persistence.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * JPA сущность для хранения обработанных событий (идемпотентность).
 * Предотвращает повторную обработку дублирующих событий Kafka.
 *
 * Соответствует таблице processed_events из ТЗ:
 * - id (BIGSERIAL)
 * - event_id (VARCHAR, UNIQUE)
 * - created_at (TIMESTAMP)
 *
 * Используется для обеспечения идемпотентности согласно требованию 2.1 ТЗ.
 */
@Entity
@Table(
        name = "processed_events",
        indexes = {
                @Index(name = "idx_processed_events_event_id", columnList = "event_id", unique = true),
                @Index(name = "idx_processed_events_created_at", columnList = "created_at")
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class ProcessedEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Уникальный идентификатор события (например, eventId из Kafka сообщения)
     * Используется для проверки идемпотентности - предотвращения повторной обработки
     */
    @Column(name = "event_id", nullable = false, unique = true, length = 255)
    private String eventId;

    /**
     * Временная метка создания записи
     * Используется для очистки старых записей
     */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}