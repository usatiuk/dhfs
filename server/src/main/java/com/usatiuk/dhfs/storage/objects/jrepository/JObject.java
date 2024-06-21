package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JObject<T extends JObjectData> implements Serializable {
    protected JObject(JObjectResolver resolver, String name, String conflictResolver, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, conflictResolver, obj.getClass());
        _dataPart = obj;
    }

    protected JObject(JObjectResolver resolver, ObjectMetadata objectMetadata) {
        _resolver = resolver;
        _metaPart = objectMetadata;
    }

    public String getName() {
        return runReadLocked(ObjectMetadata::getName);
    }

    protected final ReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ObjectMetadata _metaPart;
    private final JObjectResolver _resolver;
    private T _dataPart;

    public Class<? extends ConflictResolver> getConflictResolver() {
        return runReadLocked(m -> {
            try {
                return (Class<? extends ConflictResolver>) Class.forName(m.getConflictResolver());
            } catch (ClassNotFoundException e) {
                throw new NotImplementedException(e);
            }
        });
    }

    @FunctionalInterface
    public interface ObjectMetaFn<R> {
        R apply(ObjectMetadata indexData);
    }

    @FunctionalInterface
    public interface ObjectDataFn<T, R> {
        R apply(ObjectMetadata meta, T data);
    }

    public <X> boolean isOf(Class<X> klass) {
        return runReadLocked((m) -> (klass.isAssignableFrom(m.getType())));
    }

    public <R> R runReadLocked(ObjectMetaFn<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_metaPart);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ObjectMetaFn<R> fn) {
        _lock.writeLock().lock();
        try {
            var ret = fn.apply(_metaPart);
            _resolver.notifyWrite(this);
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    private void resolveDataPart() {
        if (_dataPart == null) {
            _lock.readLock().lock();
            try {
                if (_dataPart == null) {
                    _dataPart = _resolver.resolveData(this);
                    if (!_metaPart.getType().isAssignableFrom(_dataPart.getClass()))
                        throw new NotImplementedException("Type mismatch for " + getName());
                }
            } finally {
                _lock.readLock().unlock();
            }
        }
    }

    public <R> R runReadLocked(ObjectDataFn<T, R> fn) {
        resolveDataPart();
        _lock.readLock().lock();
        try {
            return fn.apply(_metaPart, _dataPart);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ObjectDataFn<T, R> fn) {
        resolveDataPart();
        _lock.writeLock().lock();
        try {
            var ret = fn.apply(_metaPart, _dataPart);
            _resolver.notifyWrite(this);
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }
}
