package com.bankx.transfer.infrastructure.persistence.entity;

import com.bankx.transfer.domain.model.ProcessedEventType;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * JPA сущность для хранения обработанных событий (идемпотентность Kafka-консьюмеров).
 * Соответствует таблице processed_events.
 */
@Entity
@Table(name = "processed_events",
        uniqueConstraints = {
                @UniqueConstraint(name = "uq_processed_events_transfer_id_event_type", columnNames = {"transfer_id","event_type"}),
                @UniqueConstraint(name = "uq_processed_events_event_id", columnNames = {"event_id"})
        },
        indexes = {
                @Index(name = "idx_processed_events_created_at", columnList = "created_at")
        }
)

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString(onlyExplicitlyIncluded = true)
public class ProcessedEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    @Column(name = "event_id", nullable = false, unique = true, length = 255)
    private String eventId; // соответствует UNIQUE (без имени) в SQL

    @NotNull
    @Column(name = "transfer_id", nullable = false, columnDefinition = "uuid")
    private UUID transferId; // FOREIGN KEY (но без аннотации, так как связь не моделируется через объект)

    @NotNull
    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 100)
    private ProcessedEventType eventType;

    @CreationTimestamp
    @NotNull
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}