package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class JObject<T extends JObjectData> implements Serializable, Comparable<JObject<?>> {
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ObjectMetadata _metaPart;
    private final JObjectResolver _resolver;
    private final AtomicReference<T> _dataPart = new AtomicReference<>();

    // Create a new object
    protected JObject(JObjectResolver resolver, String name, UUID selfUuid, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, false, obj.getClass());
        _dataPart.set(obj);
        _metaPart.bumpVersion(selfUuid);
    }

    // Create an object from existing metadata
    protected JObject(JObjectResolver resolver, ObjectMetadata objectMetadata) {
        _resolver = resolver;
        _metaPart = objectMetadata;
    }

    static public void rwLockAll(List<JObject<?>> objects) {
        objects.stream().sorted(Comparator.comparingInt(System::identityHashCode)).forEach(JObject::rwLock);
    }

    @Override
    public int compareTo(JObject<?> o) {
        return getName().compareTo(o.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JObject<?> jObject = (JObject<?>) o;
        return Objects.equals(_metaPart.getName(), jObject._metaPart.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_metaPart.getName());
    }

    public Class<? extends JObjectData> getKnownClass() {
        return _metaPart.getKnownClass();
    }

    public void narrowClass(Class<? extends JObjectData> klass) {
        _metaPart.narrowClass(klass);
    }

    public String getName() {
        return _metaPart.getName();
    }

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

    protected boolean isDeletionCandidate() {
        return _metaPart.isDeletionCandidate();
    }

    protected boolean isResolved() {
        return _dataPart.get() != null;
    }

    public void markSeen() {
        if (!_metaPart.isSeen()) {
            runWriteLocked(ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
                m.markSeen();
                return null;
            });
        }
    }

    private void hydrateRefs() {
        _resolver.hydrateRefs(this);
    }

    private void tryRemoteResolve() {
        if (_dataPart.get() == null) {
            _lock.writeLock().lock();
            try {
                tryLocalResolve();
                if (_dataPart.get() == null) {
                    var res = _resolver.resolveDataRemote(this);
                    _metaPart.narrowClass(res.getClass());
                    _dataPart.set(res);
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
            _lock.readLock().lock();
            try {
                if (_dataPart.get() == null) {
                    var res = _resolver.resolveDataLocal(this);
                    if (res.isEmpty()) return;

                    if (_metaPart.getSavedRefs() != null && !_metaPart.getSavedRefs().isEmpty())
                        throw new IllegalStateException("Object " + getName() + " has non-hydrated refs when written locally");

                    _metaPart.narrowClass(res.get().getClass());
                    _dataPart.compareAndSet(null, res.get());
                } // _dataPart.get() == null
            } finally {
                _lock.readLock().unlock();
            } // try
        } // _dataPart.get() == null
    }

    public boolean hasLocalCopy() {
        // FIXME: Read/write lock assert?
        return _resolver.hasLocalCopy(this);
    }

    public void externalResolution(T data) {
        assertRWLock();
        if (_dataPart.get() != null)
            throw new IllegalStateException("Data is not null when recording external resolution of " + getName());
        if (!data.getClass().isAnnotationPresent(PushResolution.class))
            throw new IllegalStateException("Expected external resolution only for classes with pushResolution " + getName());
        _metaPart.narrowClass(data.getClass());
        _dataPart.set(data);
        if (!_metaPart.isLocked())
            _metaPart.lock();
        hydrateRefs();
        verifyRefs();
    }

    public <R> R runReadLocked(ResolutionStrategy resolutionStrategy, ObjectFnRead<T, R> fn) {
        tryResolve(resolutionStrategy);

        _lock.readLock().lock();
        try {
            if (_metaPart.isDeleted())
                throw new DeletedObjectAccessException();
            return fn.apply(_metaPart, _dataPart.get());
        } finally {
            _lock.readLock().unlock();
        }
    }

    private void verifyRefs() {
        _resolver.verifyRefs(this, null);
    }

    private void verifyRefs(HashSet<String> oldRefs) {
        _resolver.verifyRefs(this, oldRefs);
    }

    public boolean hasRef(String ref) {
        return _metaPart.checkRef(ref);
    }

    public <R> R runWriteLocked(ResolutionStrategy resolutionStrategy, ObjectFnWrite<T, R> fn) {
        _lock.writeLock().lock();
        try {
            tryResolve(resolutionStrategy);

            var dataHash = _metaPart.dataHash();
            var metaHash = Objects.hash(_metaPart.metaHash(), dataHash);
            var prevData = _dataPart.get() != null || hasLocalCopy();

            HashSet<String> oldRefs = null;

            if (_resolver.refVerification) {
                tryLocalResolve();
                if (_dataPart.get() != null)
                    oldRefs = new HashSet<>(_dataPart.get().extractRefs());
            }

            VoidFn invalidateFn = () -> {
                tryLocalResolve();
                _resolver.backupRefs(this);
                _dataPart.set(null);
                _resolver.removeLocal(this, _metaPart.getName());
            };
            var ret = fn.apply(_metaPart, _dataPart.get(), this::bumpVer, invalidateFn);
            _resolver.updateDeletionState(this);

            var newDataHash = _metaPart.dataHash();
            var newMetaHash = Objects.hash(_metaPart.metaHash(), newDataHash);
            var newData = _dataPart.get() != null || hasLocalCopy();

            if (_dataPart.get() != null)
                _metaPart.narrowClass(_dataPart.get().getClass());

            if (!Objects.equals(newMetaHash, metaHash)
                    || newData != prevData)
                notifyWriteMeta();
            if (!Objects.equals(newDataHash, dataHash)
                    || newData != prevData)
                notifyWriteData();
            verifyRefs(oldRefs);
            return ret;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public boolean tryResolve(ResolutionStrategy resolutionStrategy) {
        if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
        else if (resolutionStrategy == ResolutionStrategy.REMOTE) tryRemoteResolve();

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
        _metaPart.setSavedRefs(Collections.emptySet());
    }

    public void assertRWLock() {
        if (!_lock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected to be write-locked there: " + getName() + " " + Thread.currentThread().getName());
    }

    public enum ResolutionStrategy {
        NO_RESOLUTION,
        LOCAL_ONLY,
        REMOTE
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
}
