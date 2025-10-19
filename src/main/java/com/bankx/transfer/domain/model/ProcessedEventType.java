package com.bankx.transfer.domain.model;

public enum ProcessedEventType {
    TRANSFER_COMMAND,
    DEBIT_CONFIRMED,
    DEBIT_FAILED,
    CREDIT_CONFIRMED,
    CREDIT_FAILED,
}
