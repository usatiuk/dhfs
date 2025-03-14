package com.usatiuk.dhfs.objects.transaction;

public class TxCommitException extends RuntimeException {
    public TxCommitException(String message) {
        super(message);
    }

    public TxCommitException(String message, Throwable cause) {
        super(message, cause);
    }
}
