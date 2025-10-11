package com.bankx.transfer.domain.repository;

import com.bankx.transfer.domain.model.Transfer;
import com.bankx.transfer.domain.model.TransferStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Доменный порт для работы с переводами.
 * Определяет контракты для операций хранения и извлечения переводов.
 * Отвечает за:
 * - Абстракцию доступа к данным переводов
 * - Сохранение и восстановление агрегатов Transfer
 * - Поиск переводов по различным критериям
 * - Обеспечение целостности данных на уровне домена
 */
public interface TransferRepository {

    /**
     * Сохранение или обновление перевода.
     * @param transfer доменная модель перевода
     * @return сохраненный перевод
     */
    Transfer save(Transfer transfer);

    /**
     * Поиск перевода по идентификатору.
     * @param id UUID перевода
     * @return Optional с переводом, если найден
     */
    Optional<Transfer> findById(UUID id);

    /**
     * Поиск перевода по correlationId.
     * @param correlationId идентификатор корреляции
     * @return Optional с переводом, если найден
     */
    Optional<Transfer> findByCorrelationId(UUID correlationId);

    /**
     * Поиск переводов по номеру счета отправителя.
     * @param accountNumber номер счета
     * @return список переводов
     */
    List<Transfer> findByFromAccount(String accountNumber);

    /**
     * Поиск переводов по номеру счета получателя.
     * @param accountNumber номер счета
     * @return список переводов
     */
    List<Transfer> findByToAccount(String accountNumber);

    /**
     * Поиск переводов по статусу.
     * @param status статус перевода
     * @return список переводов с указанным статусом
     */
    List<Transfer> findByStatus(TransferStatus status);

    /**
     * Проверка существования перевода по correlationId.
     * @param correlationId идентификатор корреляции
     * @return true если перевод существует
     */
    boolean existsByCorrelationId(UUID correlationId);

    /**
     * Удаление перевода (в основном для тестовых целей).
     * @param transfer перевод для удаления
     */
    void delete(Transfer transfer);
}