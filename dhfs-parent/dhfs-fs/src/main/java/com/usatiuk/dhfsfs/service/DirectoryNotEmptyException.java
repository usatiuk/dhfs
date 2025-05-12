package com.usatiuk.dhfsfs.service;

/**
 * DirectoryNotEmptyException is thrown when a directory is not empty.
 * This exception is used to indicate that a directory cannot be deleted
 * because it contains files or subdirectories.
 */
public class DirectoryNotEmptyException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
