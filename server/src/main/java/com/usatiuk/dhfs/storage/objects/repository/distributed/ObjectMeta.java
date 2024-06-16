package com.usatiuk.dhfs.storage.objects.repository.distributed;

import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ObjectMeta implements Serializable {
    public ObjectMeta(String name, Boolean assumeUnique) {
        _data = new ObjectMetaData(name, assumeUnique);
    }

    private final ObjectMetaData _data;
    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public boolean getAssumeUnique() {
        return runReadLocked((data) -> data.getAssumeUnique());
    }

    @FunctionalInterface
    public interface ObjectMetaFn<R> {
        R apply(ObjectMetaData indexData);
    }

    public <R> R runReadLocked(ObjectMetaFn<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ObjectMetaFn<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.writeLock().unlock();
        }
    }
}
