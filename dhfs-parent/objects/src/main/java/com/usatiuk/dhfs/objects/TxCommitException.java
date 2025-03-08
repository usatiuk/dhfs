package com.usatiuk.dhfs.objects;

public class TxCommitException extends RuntimeException {
    public TxCommitException(String message) {
        super(message);
    }

    public TxCommitException(String message, Throwable cause) {
        super(message, cause);
    }
}
