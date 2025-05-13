package com.usatiuk.utils;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Allows to lock arbitrary keys.
 */
public class DataLocker {
    private final ConcurrentHashMap<Object, WeakReference<ReentrantLock>> _locks = new ConcurrentHashMap<>();
    private static final Cleaner CLEANER = Cleaner.create();

    private Lock getTag(Object data) {
        var newTag = new ReentrantLock();
        var newTagRef = new WeakReference<>(newTag);

        while (true) {
            var oldTagRef = _locks.putIfAbsent(data, newTagRef);
            var oldTag = oldTagRef != null ? oldTagRef.get() : null;

            if (oldTag == null && oldTagRef != null) {
                _locks.remove(data, oldTagRef);
                continue;
            }

            if (oldTag != null)
                return oldTag;

            CLEANER.register(newTag, () -> {
                _locks.remove(data, newTagRef);
            });
            return newTag;
        }
    }

    /**
     * Locks the data and returns an AutoCloseable that unlocks it when closed.
     *
     * @param data the data to lock
     * @return an AutoCloseable that unlocks the data when closed
     */
    @Nonnull
    public AutoCloseableNoThrow lock(Object data) {
        var lock = getTag(data);
        lock.lock();
        return lock::unlock;
    }

    /**
     * Tries to lock the data and returns an AutoCloseable that unlocks it when closed.
     * If the lock is not acquired, returns null.
     *
     * @param data the data to lock
     * @return an AutoCloseable that unlocks the data when closed, or null if the lock was not acquired
     */
    @Nullable
    public AutoCloseableNoThrow tryLock(Object data) {
        var lock = getTag(data);
        if (lock.tryLock()) {
            return lock::unlock;
        } else {
            return null;
        }
    }
}
