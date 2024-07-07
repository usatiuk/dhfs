package com.usatiuk.dhfs.objects.repository;

import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentRemoteHosts implements Serializable {
    @Serial
    private static final long serialVersionUID = 1;

    private final PersistentRemoteHostsData _data = new PersistentRemoteHostsData();
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    @FunctionalInterface
    public interface PersistentRemoteHostsFn<R> {
        R apply(PersistentRemoteHostsData hostsData);
    }

    public <R> R runReadLocked(PersistentRemoteHostsFn<R> fn) {
        if (_lock.isWriteLockedByCurrentThread()) throw new IllegalStateException("Deadlock avoided!");
        _lock.readLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(PersistentRemoteHostsFn<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.writeLock().unlock();
        }
    }
}
