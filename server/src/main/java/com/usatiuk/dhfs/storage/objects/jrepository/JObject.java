package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JObject<T extends JObjectData> implements Serializable {
    protected JObject(JObjectResolver resolver, String name, String conflictResolver, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, conflictResolver, obj.getClass());
        _dataPart.set(obj);
        // FIXME:?
        _resolver.bumpVersionSelf(this);
    }

    protected JObject(JObjectResolver resolver, ObjectMetadata objectMetadata) {
        _resolver = resolver;
        _metaPart = objectMetadata;
    }

    public String getName() {
        return runReadLocked(ObjectMetadata::getName);
    }

    protected final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ObjectMetadata _metaPart;
    private final JObjectResolver _resolver;
    private final AtomicReference<T> _dataPart = new AtomicReference<>();

    public Class<? extends ConflictResolver> getConflictResolver() {
        return runReadLocked(m -> {
            try {
                return (Class<? extends ConflictResolver>) Class.forName(m.getConflictResolver(), true, JObject.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new NotImplementedException(e);
            }
        });
    }

    public boolean isResolved() {
        return _dataPart.get() != null;
    }

    @FunctionalInterface
    public interface VoidFn {
        void apply();
    }

    @FunctionalInterface
    public interface ObjectMetaFn<R> {
        R apply(ObjectMetadata indexData);
    }

    @FunctionalInterface
    public interface ObjectDataFn<T, R> {
        R apply(ObjectMetadata meta, T data);
    }

    @FunctionalInterface
    public interface ObjectMetaFnW<R> {
        R apply(ObjectMetadata indexData, VoidFn bump, VoidFn invalidate);
    }

    @FunctionalInterface
    public interface ObjectDataFnW<T, R> {
        R apply(ObjectMetadata meta, T data, VoidFn bump);
    }

    public <X> boolean isOf(Class<X> klass) {
        return (klass.isAssignableFrom(_metaPart.getType()));
    }

    public <R> R runReadLocked(ObjectMetaFn<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_metaPart);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLockedMeta(ObjectMetaFnW<R> fn) {
        _lock.writeLock().lock();
        try {
            var ver = _metaPart.getOurVersion();
            VoidFn invalidateFn = () -> {
                _dataPart.set(null);
                _resolver.removeLocal(this, _metaPart.getName());
            };
            var ret = fn.apply(_metaPart, () -> _resolver.bumpVersionSelf(this), invalidateFn);
            if (!Objects.equals(ver, _metaPart.getOurVersion()))
                _resolver.notifyWrite(this);
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    private void resolveDataPart() {
        if (_dataPart.get() == null) {
            _lock.readLock().lock();
            try {
                if (_dataPart.get() == null) {
                    _dataPart.compareAndSet(null, _resolver.resolveData(this));
                    if (!_metaPart.getType().isAssignableFrom(_dataPart.get().getClass()))
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
            return fn.apply(_metaPart, _dataPart.get());
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ObjectDataFnW<T, R> fn) {
        resolveDataPart();
        _lock.writeLock().lock();
        try {
            var ver = _metaPart.getOurVersion();
            var ret = fn.apply(_metaPart, _dataPart.get(), () -> _resolver.bumpVersionSelf(this));
            if (!Objects.equals(ver, _metaPart.getOurVersion()))
                _resolver.notifyWrite(this);
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public void assertRWLock() {
        if (!_lock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected to be write-locked there: " + getName() + " " + Thread.currentThread().getName());
    }
}
