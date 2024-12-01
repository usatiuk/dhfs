package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Optional;

// The transaction interface actually used by user code to retrieve objects
public interface Transaction {
    <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key, LockingStrategy strategy);

    <T extends JData> void putObject(JData obj);

    default <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key) {
        return getObject(type, key, LockingStrategy.READ_ONLY);
    }
}
