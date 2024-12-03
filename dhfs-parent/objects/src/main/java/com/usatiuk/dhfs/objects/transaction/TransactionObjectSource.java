package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;

public interface TransactionObjectSource {
    interface TransactionObject<T extends JData> {
        T data();

        ReadWriteLock lock();
    }

    <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key);

    <T extends JData> Optional<TransactionObject<T>> getReadLocked(Class<T> type, JObjectKey key);

    <T extends JData> Optional<TransactionObject<T>> getWriteLocked(Class<T> type, JObjectKey key);
}
