package com.usatiuk.dhfs.objects.repository;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransientPeersState {
    private final TransientPeersStateData _data = new TransientPeersStateData();
    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public <R> R runReadLocked(TransientPeersStaten<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(TransientPeersStaten<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    @FunctionalInterface
    public interface TransientPeersStaten<R> {
        R apply(TransientPeersStateData hostsData);
    }
}
