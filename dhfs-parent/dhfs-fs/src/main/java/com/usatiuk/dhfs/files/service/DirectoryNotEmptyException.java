package com.usatiuk.dhfs.files.service;

public class DirectoryNotEmptyException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
