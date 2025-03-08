package com.usatiuk.dhfs.utils;

public interface AutoCloseableNoThrow extends AutoCloseable {
    @Override
    void close();
}
