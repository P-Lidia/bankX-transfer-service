package com.bankx.transfer.infrastructure.persistence.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * JPA сущность для хранения исходящих событий в паттерне Outbox.
 */
@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_events_status_created", columnList = "status, created_at"),
                @Index(name = "idx_outbox_events_aggregate", columnList = "aggregate_type, aggregate_id"),
                @Index(name = "idx_outbox_events_event_type", columnList = "event_type"),
                @Index(name = "idx_outbox_events_correlation_id", columnList = "correlation_id")
        }
)
public class OutboxEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank(message = "Aggregate type is required")
    @Size(max = 100, message = "Aggregate type must not exceed 100 characters")
    @Column(name = "aggregate_type", nullable = false, length = 100)
    private String aggregateType;

    @NotNull(message = "Aggregate ID is required")
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @NotBlank(message = "Event type is required")
    @Size(max = 100, message = "Event type must not exceed 100 characters")
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @NotBlank(message = "Payload is required")
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private String payload;

    @NotBlank(message = "Correlation ID is required")
    @Size(max = 255, message = "Correlation ID must not exceed 255 characters")
    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @NotBlank(message = "Status is required")
    @Pattern(
            regexp = "NEW|PROCESSING|SENT|FAILED",
            message = "Status must be one of: NEW, PROCESSING, SENT, FAILED"
    )
    @Column(name = "status", nullable = false, length = 20)
    private String status = "NEW";

    @NotNull(message = "Created at timestamp is required")
    @PastOrPresent(message = "Created at must be in the past or present")
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    @Min(value = 0, message = "Retry count cannot be negative")
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Size(max = 1000, message = "Error message must not exceed 1000 characters")
    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Конструкторы

    public OutboxEventEntity() {
        // JPA requires no-args constructor
    }

    public OutboxEventEntity(String aggregateType, UUID aggregateId, String eventType,
                             String payload, String correlationId) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.status = "NEW";
        this.retryCount = 0;
        this.createdAt = LocalDateTime.now();
    }

    // Бизнес-методы

    public void markAsProcessing() {
        this.status = "PROCESSING";
        this.updatedAt = LocalDateTime.now();
    }

    public void markAsSent(LocalDateTime processedAt) {
        this.status = "SENT";
        this.processedAt = processedAt;
        this.updatedAt = LocalDateTime.now();
        this.errorMessage = null;
    }

    public void markAsFailed(String errorMessage) {
        this.status = "FAILED";
        this.retryCount++;
        this.errorMessage = errorMessage;
        this.updatedAt = LocalDateTime.now();
    }

    public boolean canRetry(int maxRetries) {
        return "FAILED".equals(this.status) && this.retryCount < maxRetries;
    }

    public boolean isCompleted() {
        return "SENT".equals(this.status);
    }

    public boolean requiresProcessing() {
        return "NEW".equals(this.status) || "FAILED".equals(this.status);
    }

    // Геттеры и сеттеры

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        if (!isValidStatus(status)) {
            throw new IllegalArgumentException("Invalid status: " + status +
                    ". Must be one of: NEW, PROCESSING, SENT, FAILED");
        }
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        if (retryCount < 0) {
            throw new IllegalArgumentException("Retry count cannot be negative");
        }
        this.retryCount = retryCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    // Вспомогательные методы

    private boolean isValidStatus(String status) {
        return status != null &&
                (status.equals("NEW") || status.equals("PROCESSING") ||
                        status.equals("SENT") || status.equals("FAILED"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OutboxEventEntity)) return false;
        OutboxEventEntity that = (OutboxEventEntity) o;
        return id != null && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return "OutboxEventEntity{" +
                "id=" + id +
                ", aggregateType='" + aggregateType + '\'' +
                ", aggregateId=" + aggregateId +
                ", eventType='" + eventType + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", status='" + status + '\'' +
                ", retryCount=" + retryCount +
                ", createdAt=" + createdAt +
                '}';
    }
}