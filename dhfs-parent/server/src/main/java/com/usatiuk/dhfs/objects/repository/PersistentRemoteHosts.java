package com.usatiuk.dhfs.objects.repository;

import lombok.Getter;

import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentRemoteHosts implements Serializable {
    @Serial
    private static final long serialVersionUID = 1;

    @Getter
    private final PersistentRemoteHostsData _data = new PersistentRemoteHostsData();
    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public <R> R runReadLocked(PersistentRemoteHostsFn<R> fn) {
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

    @FunctionalInterface
    public interface PersistentRemoteHostsFn<R> {
        R apply(PersistentRemoteHostsData hostsData);
    }
}
