package com.usatiuk.dhfsfs.service;

public class DirectoryNotEmptyException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
