package com.bankx.transfer.infrastructure.persistence.mapper;

import com.bankx.transfer.domain.model.Money;
import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.domain.model.TransferStatus;
import com.bankx.transfer.infrastructure.persistence.entity.TransferEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Currency;

/**
 * Маппер для преобразования между доменной моделью Transfer и JPA сущностью TransferEntity.
 * Отвечает за корректное преобразование типов данных и бизнес-логики между слоями.
 * Гарантирует сохранение целостности данных при миграции между доменной моделью и persistence слоем.
 *
 * Реализует паттерн Data Mapper для разделения ответственности между
 * бизнес-логикой и инфраструктурой хранения данных.
 */
@Component
@Slf4j
public class TransferMapper {

    /**
     * Преобразует доменную модель Transfer в JPA сущность TransferEntity.
     * Используется при сохранении перевода в базу данных.
     * Гарантирует соответствие структуры таблице transfers.
     *
     * @param transfer доменная модель перевода
     * @return JPA сущность для сохранения в таблицу transfers
     * @throws IllegalArgumentException если transfer null или содержит некорректные данные
     */
    public TransferEntity toEntity(Transfer transfer) {
        if (transfer == null) {
            log.warn("Attempt to map null Transfer to entity");
            return null;
        }

        log.debug("Mapping Transfer to entity: id={}", transfer.getId());

        try {
            return TransferEntity.builder()
                    .id(transfer.getId())
                    .correlationId(transfer.getCorrelationId())
                    .fromAccount(transfer.getFromAccount())
                    .toAccount(transfer.getToAccount())
                    .amount(transfer.getAmount().getAmount()) // Используем getAmount()
                    .currency(transfer.getAmount().getCurrencyCode()) // Используем getCurrencyCode()
                    .status(transfer.getStatus()) // Теперь храним enum
                    .description(transfer.getDescription())
                    .createdAt(transfer.getCreatedAt())
                    .updatedAt(transfer.getUpdatedAt())
                    .completedAt(transfer.getCompletedAt())
                    .debitTransactionId(transfer.getDebitTransactionId())
                    .creditTransactionId(transfer.getCreditTransactionId())
                    .build();

        } catch (Exception e) {
            log.error("Failed to map Transfer to entity: id={}, error={}",
                    transfer.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map Transfer to entity", e);
        }
    }

    /**
     * Преобразует JPA сущность TransferEntity в доменную модель Transfer.
     * Используется при чтении данных из БД для бизнес-логики.
     * Гарантирует, что все обязательные поля будут инициализированы.
     *
     * @param entity JPA сущность из таблицы transfers
     * @return доменная модель Transfer
     * @throws IllegalArgumentException если entity null или содержит некорректные данные
     */
    public Transfer toDomain(TransferEntity entity) {
        if (entity == null) {
            log.warn("Attempt to map null TransferEntity to domain");
            return null;
        }

        log.debug("Mapping TransferEntity to domain: id={}", entity.getId());

        try {
            // Создаем Money объект из суммы и валюты
            Money amount = Money.of(entity.getAmount(), entity.getCurrency());

            // Создаем доменную модель с помощью builder
            return Transfer.restoreBuilder()
                    .id(entity.getId())
                    .correlationId(entity.getCorrelationId())
                    .fromAccount(entity.getFromAccount())
                    .toAccount(entity.getToAccount())
                    .amount(amount)
                    .description(entity.getDescription())
                    .status(entity.getStatus()) // Используем напрямую enum
                    .createdAt(entity.getCreatedAt())
                    .updatedAt(entity.getUpdatedAt())
                    .completedAt(entity.getCompletedAt())
                    .debitTransactionId(entity.getDebitTransactionId())
                    .creditTransactionId(entity.getCreditTransactionId())
                    .build();

        } catch (Exception e) {
            log.error("Failed to map TransferEntity to domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to map TransferEntity to domain", e);
        }
    }

    /**
     * Обновляет существующую JPA сущность из доменной модели.
     * Используется при обновлении переводов в базе данных.
     * Сохраняет неизменяемые поля (id, createdAt) и обновляет изменяемые.
     *
     * @param domain доменная модель с обновленными данными
     * @param entity существующая JPA сущность для обновления
     * @throws IllegalArgumentException если domain или entity null
     */
    public void updateEntityFromDomain(Transfer domain, TransferEntity entity) {
        if (domain == null) {
            throw new IllegalArgumentException("Transfer domain cannot be null for update");
        }
        if (entity == null) {
            throw new IllegalArgumentException("TransferEntity cannot be null for update");
        }

        log.debug("Updating TransferEntity from domain: id={}", entity.getId());

        try {
            // Обновляем только изменяемые поля
            entity.setDescription(domain.getDescription());
            entity.setStatus(domain.getStatus());
            entity.setUpdatedAt(domain.getUpdatedAt());
            entity.setCompletedAt(domain.getCompletedAt());
            entity.setDebitTransactionId(domain.getDebitTransactionId());
            entity.setCreditTransactionId(domain.getCreditTransactionId());

            log.debug("Successfully updated TransferEntity from domain: id={}", entity.getId());

        } catch (Exception e) {
            log.error("Failed to update TransferEntity from domain: id={}, error={}",
                    entity.getId(), e.getMessage(), e);
            throw new IllegalArgumentException("Failed to update TransferEntity from domain", e);
        }
    }

    /**
     * Копирует данные из одной сущности в другую (клонирование).
     * Используется для создания новых записей на основе существующих.
     * Не копирует идентификатор (id) и временные метки.
     *
     * @param source исходная сущность для копирования
     * @param target целевая сущность для заполнения
     */
    public void copyEntityData(TransferEntity source, TransferEntity target) {
        if (source == null || target == null) {
            log.warn("Attempt to copy null entities");
            return;
        }

        target.setCorrelationId(source.getCorrelationId());
        target.setFromAccount(source.getFromAccount());
        target.setToAccount(source.getToAccount());
        target.setAmount(source.getAmount());
        target.setCurrency(source.getCurrency());
        target.setDescription(source.getDescription());
        target.setStatus(source.getStatus());
        target.setDebitTransactionId(source.getDebitTransactionId());
        target.setCreditTransactionId(source.getCreditTransactionId());
        // createdAt и updatedAt будут установлены при сохранении
    }

    /**
     * Проверяет соответствие доменной модели и JPA сущности.
     * Используется для валидации данных перед сохранением.
     *
     * @param domain доменная модель
     * @param entity JPA сущность
     * @return true если данные соответствуют, false в противном случае
     */
    public boolean validateMapping(Transfer domain, TransferEntity entity) {
        if (domain == null && entity == null) {
            return true;
        }
        if (domain == null || entity == null) {
            return false;
        }

        return domain.getId().equals(entity.getId()) &&
                domain.getCorrelationId().equals(entity.getCorrelationId()) &&
                domain.getFromAccount().equals(entity.getFromAccount()) &&
                domain.getToAccount().equals(entity.getToAccount()) &&
                domain.getAmount().getAmount().compareTo(entity.getAmount()) == 0 &&
                domain.getAmount().getCurrencyCode().equals(entity.getCurrency());
    }
}