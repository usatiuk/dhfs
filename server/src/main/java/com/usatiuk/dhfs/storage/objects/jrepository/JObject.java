package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JObject<T extends JObjectData> implements Serializable {
    public static class DeletedObjectAccessException extends RuntimeException {
    }

    // Create a new object
    protected JObject(JObjectResolver resolver, String name, UUID selfUuid, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, false);
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
        if (_dataPart.get() == null) throw new NotImplementedException("Data part not found!");
        return _dataPart.get().getConflictResolver();
    }

    protected boolean isDeleted() {
        return _metaPart.isDeleted();
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

    private void hydrateRefs() {
        _resolver.hydrateRefs(this);
    }

    private void resolveDataPart() {
        if (_dataPart.get() == null) {
            _lock.writeLock().lock();
            try {
                if (_dataPart.get() == null) {
                    _dataPart.set(_resolver.resolveData(this));
                    hydrateRefs();
                    verifyRefs();
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
                    hydrateRefs();
                    verifyRefs();
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
            if (_metaPart.isDeleted()) {
                Log.error("Reading deleted object " + getName());
                throw new DeletedObjectAccessException();
            }
            return fn.apply(_metaPart, _dataPart.get());
        } finally {
            _lock.readLock().unlock();
        }
    }

    private void verifyRefs() {
        _resolver.verifyRefs(this);
    }

    public <R> R runWriteLocked(ResolutionStrategy resolutionStrategy, ObjectFnWrite<T, R> fn) {
        _lock.writeLock().lock();
        try {
            if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
            else if (resolutionStrategy == ResolutionStrategy.REMOTE) resolveDataPart();

            var ver = _metaPart.getOurVersion();
            var ref = _metaPart.getRefcount();
            boolean wasSeen = _metaPart.isSeen();
            boolean wasDeleted = _metaPart.isDeleted();
            VoidFn invalidateFn = () -> {
                _resolver.backupRefs(this);
                _dataPart.set(null);
                _resolver.removeLocal(this, _metaPart.getName());
            };
            var ret = fn.apply(_metaPart, _dataPart.get(), this::bumpVer, invalidateFn);
            _resolver.updateDeletionState(this);
            if (!Objects.equals(ver, _metaPart.getOurVersion())
                    || ref != _metaPart.getRefcount()
                    || wasDeleted != _metaPart.isDeleted()
                    || wasSeen != _metaPart.isSeen())
                notifyWriteMeta();
            if (!Objects.equals(ver, _metaPart.getOurVersion()))
                notifyWriteData();
            verifyRefs();
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public boolean tryResolve(ResolutionStrategy resolutionStrategy) {
        assertRWLock();

        if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
        else if (resolutionStrategy == ResolutionStrategy.REMOTE) resolveDataPart();

        return _dataPart.get() != null;
    }

    public void notifyWriteMeta() {
        assertRWLock();
        _resolver.notifyWriteMeta(this);
    }

    public void notifyWriteData() {
        assertRWLock();
        _resolver.notifyWriteData(this);
    }

    public void notifyWrite() {
        _resolver.updateDeletionState(this);
        notifyWriteMeta();
        notifyWriteData();
    }

    public void bumpVer() {
        assertRWLock();
        _resolver.bumpVersionSelf(this);
    }

    public void rwLock() {
        _lock.writeLock().lock();
    }

    public boolean tryRwLock() {
        return _lock.writeLock().tryLock();
    }

    public void rwUnlock() {
        _lock.writeLock().unlock();
    }

    public void discardData() {
        assertRWLock();
        if (!isDeleted())
            throw new IllegalStateException("Expected to be deleted when discarding data");
        _dataPart.set(null);
    }

    static public void rwLockAll(List<JObject<?>> objects) {
        objects.stream().sorted(Comparator.comparingInt(System::identityHashCode)).forEach(JObject::rwLock);
    }

    public void assertRWLock() {
        if (!_lock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected to be write-locked there: " + getName() + " " + Thread.currentThread().getName());
    }
}
