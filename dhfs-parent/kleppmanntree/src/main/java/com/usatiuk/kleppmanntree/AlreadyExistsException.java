package com.usatiuk.kleppmanntree;

/**
 * Exception thrown when an attempt is made to create a new tree node as a child with a name that already exists.
 */
public class AlreadyExistsException extends RuntimeException {
    public AlreadyExistsException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
