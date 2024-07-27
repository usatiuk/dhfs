package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.utils.VoidFn;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class JObject<T extends JObjectData> {
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private final ObjectMetadata _metaPart;
    private final JObjectResolver _resolver;
    private final AtomicReference<T> _dataPart = new AtomicReference<>();
    private static final int lockTimeoutSecs = 15;

    private class TransactionState {
        final int dataHash;
        final int metaHash;
        final int externalHash;
        final boolean data;
        final HashSet<String> oldRefs;

        TransactionState() {
            this.dataHash = _metaPart.dataHash();
            this.metaHash = _metaPart.metaHash();
            this.externalHash = _metaPart.externalHash();
            this.data = _dataPart.get() != null || hasLocalCopy();

            if (_resolver.refVerification) {
                tryLocalResolve();
                if (_dataPart.get() != null)
                    oldRefs = new HashSet<>(_dataPart.get().extractRefs());
                else
                    oldRefs = null;
            } else {
                oldRefs = null;
            }
        }

        void commit(boolean forceInvalidate) {
            _resolver.updateDeletionState(JObject.this);

            var newDataHash = _metaPart.dataHash();
            var newMetaHash = _metaPart.metaHash();
            var newExternalHash = _metaPart.externalHash();
            var newData = _dataPart.get() != null || hasLocalCopy();

            if (_dataPart.get() != null)
                _metaPart.narrowClass(_dataPart.get().getClass());

            notifyWrite(
                    newMetaHash != metaHash || forceInvalidate,
                    newExternalHash != externalHash || forceInvalidate,
                    newDataHash != dataHash
                            || newData != data
                            || forceInvalidate
            );

            verifyRefs(oldRefs);
        }
    }

    private TransactionState _transactionState = null;

    // Create a new object
    protected JObject(JObjectResolver resolver, String name, UUID selfUuid, T obj) {
        _resolver = resolver;
        _metaPart = new ObjectMetadata(name, false, obj.getClass());
        _metaPart.setHaveLocalCopy(true);
        _dataPart.set(obj);
        _metaPart.getChangelog().put(selfUuid, 1L);
        if (Log.isTraceEnabled())
            Log.trace("new JObject: " + getName());
    }

    // Create an object from existing metadata
    protected JObject(JObjectResolver resolver, ObjectMetadata objectMetadata) {
        _resolver = resolver;
        _metaPart = objectMetadata;
        Log.trace("new JObject (ext): " + getName());
    }

    static public void rwLockAll(List<JObject<?>> objects) {
        objects.stream().sorted(Comparator.comparingInt(System::identityHashCode)).forEach(JObject::rwLock);
    }

    public Class<? extends JObjectData> getKnownClass() {
        return _metaPart.getKnownClass();
    }

    protected void narrowClass(Class<? extends JObjectData> klass) {
        _metaPart.narrowClass(klass);
    }

    public String getName() {
        return _metaPart.getName();
    }

    public T getData() {
//        assertRWLock(); FIXME:
        return _dataPart.get();
    }

    public ObjectMetadata getMeta() {
        assertRWLock();
        return _metaPart;
    }

    protected boolean hasLocalCopyMd() {
        return _metaPart.isHaveLocalCopy();
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
            rwLock();
            try {
                tryLocalResolve();
                if (_dataPart.get() == null) {
                    var res = _resolver.resolveDataRemote(this);
                    _metaPart.narrowClass(res.getClass());
                    _dataPart.set(res);
                    _metaPart.setHaveLocalCopy(true);
                    hydrateRefs();
                    verifyRefs();
                } // _dataPart.get() == null
            } finally {
                rwUnlock();
            } // try
        } // _dataPart.get() == null
    }

    private void tryLocalResolve() {
        if (_dataPart.get() == null) {
            rLock();
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
                rUnlock();
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
        _metaPart.setHaveLocalCopy(true);
        if (!_metaPart.isLocked())
            _metaPart.lock();
        hydrateRefs();
        verifyRefs();
    }

    public boolean tryRLock() {
        try {
            return _lock.readLock().tryLock(lockTimeoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tryRWLock() {
        try {
            return _lock.writeLock().tryLock(lockTimeoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void rwLock() {
        if (!tryRWLock())
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Failed to acquire write lock for " + getName()));
        if (_lock.writeLock().getHoldCount() == 1) {
            _transactionState = new TransactionState();
        }
    }

    public void rLock() {
        if (!tryRLock())
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Failed to acquire read lock for " + getName()));
    }

    public void rUnlock() {
        _lock.readLock().unlock();
    }

    public void rwUnlock() {
        rwUnlock(false);
    }

    public void rwUnlock(boolean forceInvalidate) {
        try {
            if (_lock.writeLock().getHoldCount() == 1) {
                _transactionState.commit(forceInvalidate);
                _transactionState = null;
            }
        } catch (Exception ex) {
            Log.error("When committing changes to " + getName(), ex);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public void assertRWLock() {
        if (!_lock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected to be write-locked there: " + getName() + " " + Thread.currentThread().getName());
    }

    public <R> R runReadLocked(ResolutionStrategy resolutionStrategy, ObjectFnRead<T, R> fn) {
        tryResolve(resolutionStrategy);

        rLock();
        try {
            if (_metaPart.isDeleted())
                throw new DeletedObjectAccessException();
            return fn.apply(_metaPart, _dataPart.get());
        } finally {
            rUnlock();
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
        rwLock();
        try {
            tryResolve(resolutionStrategy);
            VoidFn invalidateFn = () -> {
                tryLocalResolve();
                _resolver.backupRefs(this);
                _dataPart.set(null);
                _resolver.removeLocal(this, _metaPart.getName());
            };
            return fn.apply(_metaPart, _dataPart.get(), this::bumpVer, invalidateFn);
        } finally {
            rwUnlock();
        }
    }

    public boolean tryResolve(ResolutionStrategy resolutionStrategy) {
        if (resolutionStrategy == ResolutionStrategy.LOCAL_ONLY) tryLocalResolve();
        else if (resolutionStrategy == ResolutionStrategy.REMOTE) tryRemoteResolve();

        return _dataPart.get() != null;
    }

    private void notifyWrite(boolean metaChanged, boolean externalChanged, boolean hasDataChanged) {
        assertRWLock();
        _resolver.notifyWrite(this, metaChanged, externalChanged, hasDataChanged);
    }

    public void bumpVer() {
        assertRWLock();
        _resolver.bumpVersionSelf(this);
    }


    public void discardData() {
        assertRWLock();
        if (!isDeleted())
            throw new IllegalStateException("Expected to be deleted when discarding data");
        _dataPart.set(null);
        _metaPart.setSavedRefs(Collections.emptySet());
    }

    public enum ResolutionStrategy {
        NO_RESOLUTION,
        LOCAL_ONLY,
        REMOTE
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
