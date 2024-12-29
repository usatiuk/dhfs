package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Optional;

// The transaction interface actually used by user code to retrieve objects
public interface Transaction {
    long getId();

    <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key, LockingStrategy strategy);

    <T extends JData> void putObject(JData obj);

    void deleteObject(JObjectKey key);

    default <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key) {
        return getObject(type, key, LockingStrategy.OPTIMISTIC);
    }
}
