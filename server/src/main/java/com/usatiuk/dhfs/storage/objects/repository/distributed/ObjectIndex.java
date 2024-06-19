package com.usatiuk.dhfs.storage.objects.repository.distributed;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ObjectIndex implements Serializable {

    private final ObjectIndexData _data = new ObjectIndexData();
    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    @FunctionalInterface
    public interface ObjectIndexFn<R> {
        R apply(ObjectIndexData indexData);
    }

    public <R> R runReadLocked(ObjectIndexFn<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ObjectIndexFn<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_data);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public boolean exists(String name) {
        return runReadLocked((data) -> {
            return data.getObjectMetaMap().containsKey(name);
        });
    }

    public Optional<ObjectMeta> get(String name) {
        return runReadLocked((data) -> {
            if (data.getObjectMetaMap().containsKey(name)) {
                return Optional.of(data.getObjectMetaMap().get(name));
            } else {
                return Optional.empty();
            }
        });
    }

    public ObjectMeta getOrCreate(String name, String conflictResolver) {
        return runWriteLocked((data) -> {
            if (data.getObjectMetaMap().containsKey(name)) {
                var got = data.getObjectMetaMap().get(name);
                if (!Objects.equals(got.getConflictResolver(), conflictResolver))
                    throw new IllegalArgumentException("conflictResolver mismatch for " + name);
                return got;
            } else {
                var newObjectMeta = new ObjectMeta(name, conflictResolver);
                data.getObjectMetaMap().put(name, newObjectMeta);
                return newObjectMeta;
            }
        });
    }

}
