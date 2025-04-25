package com.usatiuk.utils;

public interface AutoCloseableNoThrow extends AutoCloseable {
    @Override
    void close();
}
