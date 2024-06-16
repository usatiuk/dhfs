package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ObjectIndex implements Serializable {
    @Getter
    final Map<String, ObjectMeta> _objectMetaMap = new HashMap<>();

    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public <R> R runReadLocked(Callable<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(Callable<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public boolean exists(String name) {
        return runReadLocked(() -> {
            return _objectMetaMap.containsKey(name);
        });
    }

    public Optional<ObjectMeta> get(String name) {
        return runReadLocked(() -> {
            if (_objectMetaMap.containsKey(name)) {
                return Optional.of(_objectMetaMap.get(name));
            } else {
                return Optional.empty();
            }
        });
    }

    public ObjectMeta getOrCreate(String name, boolean assumeUnique) {
        return runWriteLocked(() -> {
            if (_objectMetaMap.containsKey(name)) {
                var got = _objectMetaMap.get(name);
                if (got.getAssumeUnique() != assumeUnique)
                    throw new IllegalArgumentException("assumeUnique mismatch for " + name);
                return got;
            } else {
                var newObjectMeta = new ObjectMeta(name, assumeUnique);
                _objectMetaMap.put(name, newObjectMeta);
                return newObjectMeta;
            }
        });
    }

}
