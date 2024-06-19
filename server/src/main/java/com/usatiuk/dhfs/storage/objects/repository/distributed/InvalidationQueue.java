package com.usatiuk.dhfs.storage.objects.repository.distributed;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InvalidationQueue {
    private final InvalidationQueueData _data = new InvalidationQueueData();
    private final ReadWriteLock _dataLock = new ReentrantReadWriteLock();

    @FunctionalInterface
    public interface InvalidationQueueDataFn<R> {
        R apply(InvalidationQueueData data);
    }

    public <R> R runReadLocked(InvalidationQueueDataFn<R> fn) {
        _dataLock.readLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _dataLock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(InvalidationQueueDataFn<R> fn) {
        _dataLock.writeLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _dataLock.writeLock().unlock();
        }
    }

}
