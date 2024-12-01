package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;

public interface TransactionObjectSource {
    interface TransactionObject<T extends JData> {
        T get();

        ReadWriteLock getLock();
    }

    <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key);
}
