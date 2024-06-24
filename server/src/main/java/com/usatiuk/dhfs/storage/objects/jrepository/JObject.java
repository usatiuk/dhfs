package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JObject<T extends JObjectData> implements Serializable {
    // Create a new object
    protected JObject(JObjectResolver resolver, String name, String conflictResolver, UUID selfUuid, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, conflictResolver, obj.getClass());
        _dataPart.set(obj);
        // FIXME:?
        if (!obj.assumeUnique())
            _metaPart.bumpVersion(selfUuid);
    }

    // Create an object from existing metadata
    protected JObject(JObjectResolver resolver, ObjectMetadata objectMetadata) {
        _resolver = resolver;
        _metaPart = objectMetadata;
    }

    public String getName() {
        return _metaPart.getName();
    }

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ObjectMetadata _metaPart;
    private final JObjectResolver _resolver;
    private final AtomicReference<T> _dataPart = new AtomicReference<>();

    public T getData() {
        assertRWLock();
        return _dataPart.get();
    }

    public ObjectMetadata getMeta() {
        assertRWLock();
        return _metaPart;
    }

    public Class<? extends ConflictResolver> getConflictResolver() {
        try {
            return (Class<? extends ConflictResolver>) Class.forName(_metaPart.getConflictResolver(), true,
                    JObject.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new NotImplementedException(e);
        }
    }

    protected boolean isResolved() {
        return _dataPart.get() != null;
    }

    @FunctionalInterface
    public interface VoidFn {
        void apply();
    }

    @FunctionalInterface
    public interface ObjectFnRead<T, R> {
        R apply(ObjectMetadata meta, @Nullable T data);
    }

    @FunctionalInterface
    public interface ObjectFnWrite<T, R> {
        R apply(ObjectMetadata indexData, @Nullable T data, VoidFn bump, VoidFn invalidate);
    }

    public <X> boolean isOf(Class<X> klass) {
        return (klass.isAssignableFrom(_metaPart.getType()));
    }

    private void resolveDataPart() {
        if (_dataPart.get() == null) {
            _lock.writeLock().lock();
            try {
                if (_dataPart.get() == null) {
                    _dataPart.set(_resolver.resolveData(this));

                    if (!_metaPart.getType().isAssignableFrom(_dataPart.get().getClass()))
                        throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Type mismatch for " + getName()));
                } // _dataPart.get() == null
            } finally {
                _lock.writeLock().unlock();
            } // try
        } // _dataPart.get() == null
    }

    private void tryLocalResolve() {
        if (_dataPart.get() == null) {
            _lock.writeLock().lock();
            try {
                if (_dataPart.get() == null) {
                    var res = _resolver.resolveDataLocal(this);
                    if (res.isEmpty()) return;
                    _dataPart.set(res.get());

                    if (!_metaPart.getType().isAssignableFrom(_dataPart.get().getClass()))
                        throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Type mismatch for " + getName()));
                } // _dataPart.get() == null
            } finally {
                _lock.writeLock().unlock();
            } // try
        } // _dataPart.get() == null
    }

    public enum ResolutionStrategy {
        NO_RESOLUTION,
        LOCAL_ONLY,
        REMOTE
    }

    public <R> R runReadLocked(ResolutionStrategy resolutionStrategy, ObjectFnRead<T, R> fn) {
        if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
        else if (resolutionStrategy == ResolutionStrategy.REMOTE) resolveDataPart();

        _lock.readLock().lock();

        try {
            return fn.apply(_metaPart, _dataPart.get());
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(ResolutionStrategy resolutionStrategy, ObjectFnWrite<T, R> fn) {
        _lock.writeLock().lock();
        try {
            if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
            else if (resolutionStrategy == ResolutionStrategy.REMOTE) resolveDataPart();

            var ver = _metaPart.getOurVersion();
            VoidFn invalidateFn = () -> {
                _dataPart.set(null);
                _resolver.removeLocal(this, _metaPart.getName());
            };
            var ret = fn.apply(_metaPart, _dataPart.get(), () -> _resolver.bumpVersionSelf(this), invalidateFn);
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
