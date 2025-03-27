package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;

import java.util.Optional;

// The transaction interface actually used by user code to retrieve objects
public interface Transaction extends TransactionHandle {
    void onCommit(Runnable runnable);

    <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy);

    <T extends JData> void put(JData obj);

    void delete(JObjectKey key);

    default <T extends JData> Optional<T> get(Class<T> type, JObjectKey key) {
        return get(type, key, LockingStrategy.OPTIMISTIC);
    }

    CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key);

    default CloseableKvIterator<JObjectKey, JData> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }

}
