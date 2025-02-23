package com.usatiuk.dhfs.objects;

public class StaleIteratorException extends RuntimeException {
    public StaleIteratorException() {
        super();
    }

    public StaleIteratorException(String message) {
        super(message);
    }
}
