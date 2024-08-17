package com.usatiuk.dhfs.objects.jrepository;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import com.usatiuk.utils.VoidFn;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Getter;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _writeListeners
            = new ArrayListValuedHashMap<>();
    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _metaWriteListeners
            = new ArrayListValuedHashMap<>();
    private final ConcurrentHashMap<String, NamedWeakReference> _map = new ConcurrentHashMap<>();
    private final ReferenceQueue<JObject<?>> _refQueue = new ReferenceQueue<>();

    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    JObjectWriteback jObjectWriteback;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    ProtoSerializerService protoSerializerService;
    @Inject
    JObjectRefProcessor jObjectRefProcessor;
    @Inject
    JObjectLRU jObjectLRU;
    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;

    private Thread _refCleanupThread;

    @Override
    public <T extends JObjectData> void registerWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _writeListeners.put(klass, fn);
    }

    @Override
    public <T extends JObjectData> void registerMetaWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _metaWriteListeners.put(klass, fn);
    }

    @Startup
    void init() {
        _refCleanupThread = new Thread(this::refCleanupThread);
        _refCleanupThread.setName("JObject ref cleanup thread");
        _refCleanupThread.start();
    }

    @Shutdown
    void shutdown() throws InterruptedException {
        _refCleanupThread.interrupt();
        _refCleanupThread.join();
    }

    private void refCleanupThread() {
        try {
            while (!Thread.interrupted()) {
                NamedWeakReference cur = (NamedWeakReference) _refQueue.remove();
                _map.remove(cur._key, cur);
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Ref cleanup thread exiting");
    }

    private JObject<?> getFromMap(String key) {
        var ret = _map.get(key);
        if (ret != null && ret.get() != null) {
            return ret.get();
        }
        return null;
    }

    @Override
    public Optional<JObjectManager.JObject<?>> get(String name) {
        {
            var inMap = getFromMap(name);
            if (inMap != null) {
                jObjectLRU.notifyAccess(inMap);
                return Optional.of(inMap);
            }
        }

        ObjectMetadataP readMd;
        try {
            readMd = objectPersistentStore.readObjectMeta(name);
        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode().equals(Status.NOT_FOUND.getCode()))
                return Optional.empty();
            throw ex;
        }
        var meta = protoSerializerService.deserialize(readMd);
        if (!(meta instanceof ObjectMetadata))
            throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Unexpected metadata type for " + name));

        if (((ObjectMetadata) meta).isDeleted()) {
            Log.warn("Deleted meta on disk for " + name);
            return Optional.empty();
        }

        JObject<?> ret = null;
        var newObj = new JObject<>((ObjectMetadata) meta);
        while (ret == null) {
            var ref = _map.computeIfAbsent(name, k -> new NamedWeakReference(newObj, _refQueue));
            if (ref.get() == null) _map.remove(name, ref);
            else ret = ref.get();
        }
        jObjectLRU.notifyAccess(ret);
        return Optional.of(ret);
    }

    @Override
    public Collection<String> findAll() {
        var out = _map.values().stream().map(WeakReference::get)
                .filter(Objects::nonNull)
                .map(JObject::getMeta).map(ObjectMetadata::getName)
                .collect(Collectors.toCollection((Supplier<LinkedHashSet<String>>) LinkedHashSet::new));
        out.addAll(objectPersistentStore.findAllObjects());
        return out;
    }

    @Override
    public <D extends JObjectData> JObject<D> putLocked(D object, Optional<String> parent) {
        while (true) {
            JObject<?> ret;
            JObject<?> newObj = null;
            try {
                ret = getFromMap(object.getName());
                if (ret != null) {
                    if (!object.getClass().isAnnotationPresent(AssumedUnique.class))
                        throw new IllegalArgumentException("Trying to insert different object with same key");
                } else {
                    newObj = new JObject<D>(object.getName(), persistentPeerDataService.getSelfUuid(), object);
                    newObj.rwLock();
                    while (ret == null) {
                        JObject<?> finalNewObj = newObj;
                        var ref = _map.computeIfAbsent(object.getName(), k -> new NamedWeakReference(finalNewObj, _refQueue));
                        if (ref.get() == null) _map.remove(object.getName(), ref);
                        else ret = ref.get();
                    }
                    if (ret != newObj) continue;
                }
                JObject<D> finalRet = (JObject<D>) ret;
                ret.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    if (object.getClass().isAnnotationPresent(PushResolution.class)
                            && object.getClass().isAnnotationPresent(AssumedUnique.class)
                            && finalRet.getData() == null) {
                        finalRet.externalResolution(object);
                    }

                    if (parent.isPresent()) {
                        m.addRef(parent.get());
                        if (m.isLocked())
                            m.unlock();
                    } else {
                        m.lock();
                    }

                    return null;
                });
            } finally {
                if (newObj != null)
                    newObj.forceInvalidate();
            }
            if (newObj == null) {
                jObjectLRU.notifyAccess(ret);
                ret.rwLock();
            }
            return (JObject<D>) ret;
        }
    }

    @Override
    public <D extends JObjectData> JObject<D> put(D object, Optional<String> parent) {
        var ret = putLocked(object, parent);
        ret.rwUnlock();
        return ret;
    }

    @Override
    public JObjectManager.JObject<?> getOrPutLocked(String name, Class<? extends JObjectData> klass, Optional<String> parent) {
        while (true) {
            var got = get(name).orElse(null);

            if (got != null) {
                got.rwLock();
                var meta = got.getMeta();
                meta.narrowClass(klass);
                meta.markSeen();
                parent.ifPresent(s -> got.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    if (m.isLocked())
                        m.unlock();
                    m.addRef(s);
                    return true;
                }));
                return got;
            }

            JObject<?> ret = null;
            var created = new JObject<>(new ObjectMetadata(name, false, klass));
            created.rwLock();
            while (ret == null) {
                var ref = _map.computeIfAbsent(name, k -> new NamedWeakReference(created, _refQueue));
                if (ref.get() == null) _map.remove(name, ref);
                else ret = ref.get();
            }
            if (ret != created) continue;

            created.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                parent.ifPresent(m::addRef);
                m.markSeen();
                return null;
            });
            return created;
        }
    }

    @Override
    public JObjectManager.JObject<?> getOrPut(String name, Class<? extends JObjectData> klass, Optional<String> parent) {
        var obj = getOrPutLocked(name, klass, parent);
        obj.rwUnlock();
        return obj;
    }

    private static class NamedWeakReference extends WeakReference<JObject<?>> {
        @Getter
        final String _key;

        public NamedWeakReference(JObject<?> target, ReferenceQueue<JObject<?>> q) {
            super(target, q);
            this._key = target.getMeta().getName();
        }
    }

    public class JObject<T extends JObjectData> implements JObjectManager.JObject<T> {
        private static final int lockTimeoutSecs = 15;
        private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
        private final ObjectMetadata _metaPart;
        private final AtomicReference<T> _dataPart = new AtomicReference<>();
        private TransactionState _transactionState = null;

        // Create a new object
        protected JObject(String name, UUID selfUuid, T obj) {
            _metaPart = new ObjectMetadata(name, false, obj.getClass());
            _metaPart.setHaveLocalCopy(true);
            _dataPart.set(obj);
            _metaPart.getChangelog().put(selfUuid, 1L);
            if (Log.isTraceEnabled())
                Log.trace("new JObject: " + getMeta().getName());
        }

        // Create an object from existing metadata
        protected JObject(ObjectMetadata objectMetadata) {
            _metaPart = objectMetadata;
            if (Log.isTraceEnabled())
                Log.trace("new JObject (ext): " + getMeta().getName());
        }

        @Override
        public T getData() {
            return _dataPart.get();
        }

        @Override
        public ObjectMetadata getMeta() {
            return _metaPart;
        }

        @Override
        public void markSeen() {
            if (!_metaPart.isSeen()) {
                runWriteLocked(ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
                    m.markSeen();
                    return null;
                });
            }
        }

        private void tryRemoteResolve() {
            if (_dataPart.get() == null) {
                rwLock();
                try {
                    tryLocalResolve();
                    if (_dataPart.get() == null) {
                        var res = resolveDataRemote();
                        _metaPart.narrowClass(res.getClass());
                        _dataPart.set((T) res);
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
                        if (!getMeta().isHaveLocalCopy()) return;
                        JObjectData res;
                        try {
                            res = resolveDataLocal();
                        } catch (Exception e) {
                            Log.error("Object " + _metaPart.getName() + " data couldn't be read but it should exist locally!", e);
                            return;
                        }

                        if (_metaPart.getSavedRefs() != null && !_metaPart.getSavedRefs().isEmpty())
                            throw new IllegalStateException("Object " + _metaPart.getName() + " has non-hydrated refs when written locally");

                        _metaPart.narrowClass(res.getClass());
                        if (_dataPart.compareAndSet(null, (T) res))
                            onResolution();
                    } // _dataPart.get() == null
                } finally {
                    rUnlock();
                } // try
            } // _dataPart.get() == null
        }

        @Override
        public void externalResolution(T data) {
            assertRwLock();
            if (_dataPart.get() != null)
                throw new IllegalStateException("Data is not null when recording external resolution of " + getMeta().getName());
            if (!data.getClass().isAnnotationPresent(PushResolution.class))
                throw new IllegalStateException("Expected external resolution only for classes with pushResolution " + getMeta().getName());
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
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Failed to acquire write lock for " + getMeta().getName()));
            if (_lock.writeLock().getHoldCount() == 1) {
                _transactionState = new TransactionState();
            }
        }

        public void rLock() {
            if (!tryRLock())
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Failed to acquire read lock for " + getMeta().getName()));
        }

        public void rUnlock() {
            _lock.readLock().unlock();
        }

        protected void forceInvalidate() {
            assertRwLock();
            _transactionState.forceInvalidate();
        }

        public void rwUnlock() {
            try {
                if (_lock.writeLock().getHoldCount() == 1) {
                    _transactionState.commit();
                    _transactionState = null;
                }
            } catch (Exception ex) {
                Log.error("When committing changes to " + getMeta().getName(), ex);
            } finally {
                _lock.writeLock().unlock();
            }
        }

        public boolean haveRwLock() {
            return _lock.isWriteLockedByCurrentThread();
        }

        @Override
        public void assertRwLock() {
            if (!haveRwLock())
                throw new IllegalStateException("Expected to be write-locked there: " + getMeta().getName() + " " + Thread.currentThread().getName());
        }

        @Override
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

        protected boolean isResolved() {
            return _dataPart.get() != null;
        }

        private void verifyRefs() {
            verifyRefs(null);
        }

        @Override
        public <R> R runWriteLocked(ResolutionStrategy resolutionStrategy, ObjectFnWrite<T, R> fn) {
            rwLock();
            try {
                tryResolve(resolutionStrategy);
                VoidFn invalidateFn = () -> {
                    tryLocalResolve();
                    backupRefs();
                    _dataPart.set(null);
                    removeLocal(_metaPart.getName());
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

        @Override
        public void discardData() {
            assertRwLock();
            if (!getMeta().isDeleted())
                throw new IllegalStateException("Expected to be deleted when discarding data");
            _dataPart.set(null);
            _metaPart.setHaveLocalCopy(false);
            _metaPart.setSavedRefs(new HashSet<>());
        }

        public void backupRefs() {
            assertRwLock();
            if (getData() != null) {
                if ((getMeta().getSavedRefs() != null) && (!getMeta().getSavedRefs().isEmpty())) {
                    Log.error("Saved refs not empty for " + getMeta().getName() + " will clean");
                    getMeta().setSavedRefs(null);
                }
                getMeta().setSavedRefs(new LinkedHashSet<>(getData().extractRefs()));
            }
        }

        public void hydrateRefs() {
            assertRwLock();
            if (getMeta().getSavedRefs() != null) {
                StringBuilder sb = new StringBuilder();
                sb.append("Hydrating refs for ").append(getMeta().getName()).append("\n");
                sb.append("Saved refs: ");
                getMeta().getSavedRefs().forEach(r -> sb.append(r).append(" "));
                sb.append("\nExtracted refs: ");
                var extracted = new LinkedHashSet<>(getData().extractRefs());
                extracted.forEach(r -> sb.append(r).append(" "));
                Log.debug(sb.toString());
                for (var r : getMeta().getSavedRefs()) {
                    if (!extracted.contains(r))
                        jObjectManager.get(r).ifPresent(ro -> ro.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                            m.removeRef(getMeta().getName());
                            return null;
                        }));
                }
                for (var r : extracted) {
                    if (!getMeta().getSavedRefs().contains(r)) {
                        Log.trace("Hydrating ref " + r + " for " + getMeta().getName());
                        jObjectManager.getOrPut(r, getData().getRefType(), Optional.of(getMeta().getName()));
                    }
                }
                getMeta().setSavedRefs(null);
            }
        }

        public void updateDeletionState() {
            assertRwLock();

            if (!getMeta().isDeletionCandidate() && getMeta().isDeleted()) {
                getMeta().undelete();
                Log.debug("Undelete: " + getMeta().getName());

                Stream<String> refs = Stream.empty();

                if (getMeta().getSavedRefs() != null)
                    refs = getMeta().getSavedRefs().stream();
                if (getData() != null)
                    refs = Streams.concat(refs, getData().extractRefs().stream());

                refs.forEach(r -> {
                    Log.trace("Hydrating ref after undelete " + r + " for " + getMeta().getName());
                    jObjectManager.getOrPut(r, getData() != null ? getData().getRefType() : JObjectData.class, Optional.of(getMeta().getName()));
                });

            }

            if (getMeta().isDeletionCandidate() && !getMeta().isDeleted()) {
                if (!getMeta().isSeen()) tryQuickDelete();
                else jObjectRefProcessor.putDeletionCandidate(getMeta().getName());
            }
        }

        private void quickDeleteRef(String name) {
            jObjectManager.get(name)
                    .ifPresent(ref -> ref.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
                        mc.removeRef(getMeta().getName());
                        return null;
                    }));
        }

        private void tryQuickDelete() {
            assertRwLock();
            if (!getMeta().getKnownClass().isAnnotationPresent(Leaf.class))
                tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);

            if (Log.isTraceEnabled())
                Log.trace("Quick delete of: " + getMeta().getName());

            getMeta().markDeleted();

            Collection<String> extracted = null;
            if (!getMeta().getKnownClass().isAnnotationPresent(Leaf.class) && getData() != null)
                extracted = getData().extractRefs();
            Collection<String> saved = getMeta().getSavedRefs();

            discardData();

            if (saved != null)
                for (var r : saved) quickDeleteRef(r);
            if (extracted != null)
                for (var r : extracted) quickDeleteRef(r);
        }

        public <T extends JObjectData> T resolveDataLocal() {
            // jObject.assertRwLock();
            // FIXME: No way to assert read lock?
            return protoSerializerService.deserialize(objectPersistentStore.readObject(getMeta().getName()));
        }

        public <T extends JObjectData> T resolveDataRemote() {
            var obj = remoteObjectServiceClient.getObject(this);
            jObjectWriteback.markDirty(this);
            invalidationQueueService.pushInvalidationToAll(getMeta().getName());
            return protoSerializerService.deserialize(obj);
        }

        // Really more like "onUpdateSize"
        // Also not called from tryResolveRemote/externalResolution because
        // there it's handled by the notifyWrite
        public void onResolution() {
            jObjectLRU.updateSize(this);
        }

        public void removeLocal(String name) {
            assertRwLock();
            try {
                Log.debug("Invalidating " + name);
                getMeta().setHaveLocalCopy(false);
            } catch (StatusRuntimeException sx) {
                if (sx.getStatus() != Status.NOT_FOUND)
                    Log.info("Couldn't delete object from persistent store: ", sx);
            } catch (Exception e) {
                Log.info("Couldn't delete object from persistent store: ", e);
            }
        }

        public <T extends JObjectData> void notifyWrite(boolean metaChanged,
                                                        boolean externalChanged, boolean hasDataChanged) {
            assertRwLock();
            jObjectLRU.updateSize(this);
            if (metaChanged || hasDataChanged)
                jObjectWriteback.markDirty(this);
            if (metaChanged)
                for (var t : _metaWriteListeners.keySet()) { // FIXME:?
                    if (t.isAssignableFrom(getMeta().getKnownClass()))
                        for (var cb : _metaWriteListeners.get(t))
                            cb.apply((JObject) this);
                }
            if (hasDataChanged)
                for (var t : _writeListeners.keySet()) { // FIXME:?
                    if (t.isAssignableFrom(getMeta().getKnownClass()))
                        for (var cb : _writeListeners.get(t))
                            cb.apply((JObject) this);
                }
            if (externalChanged && getMeta().isHaveLocalCopy()) {
                invalidationQueueService.pushInvalidationToAll(getMeta().getName());
            }
        }

        @Override
        public void bumpVer() {
            assertRwLock();
            runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, data, bump, invalidate) -> {
                m.bumpVersion(persistentPeerDataService.getSelfUuid());
                return null;
            });
        }

        protected void verifyRefs(@Nullable HashSet<String> oldRefs) {
            if (!refVerification) return;
            assertRwLock();
            if (!isResolved()) return;
            if (getMeta().isDeleted()) return;
            var newRefs = getData().extractRefs();
            if (oldRefs != null) for (var o : oldRefs)
                if (!newRefs.contains(o)) {
                    jObjectManager.get(o).ifPresent(obj -> {
                        if (obj.getMeta().checkRef(getMeta().getName()))
                            throw new IllegalStateException("Object " + o + " is referenced from " + getMeta().getName() + " but shouldn't be");
                    });
                }
            for (var r : newRefs) {
                var obj = jObjectManager.get(r).orElseThrow(() -> new IllegalStateException("Object " + r + " not found but should be referenced from " + getMeta().getName()));
                if (obj.getMeta().isDeleted())
                    throw new IllegalStateException("Object " + r + " deleted but referenced from " + getMeta().getName());
                if (!obj.getMeta().checkRef(getMeta().getName()))
                    throw new IllegalStateException("Object " + r + " is not referenced by " + getMeta().getName() + " but should be");
            }
        }

        private class TransactionState {
            final int dataHash;
            final int metaHash;
            final int externalHash;
            final boolean data;
            final HashSet<String> oldRefs;
            boolean forceInvalidate = false;

            TransactionState() {
                this.dataHash = _metaPart.dataHash();
                this.metaHash = _metaPart.metaHash();
                this.externalHash = _metaPart.externalHash();
                this.data = _dataPart.get() != null || getMeta().isHaveLocalCopy();

                if (refVerification) {
                    tryLocalResolve();
                    if (_dataPart.get() != null)
                        oldRefs = new HashSet<>(_dataPart.get().extractRefs());
                    else
                        oldRefs = null;
                } else {
                    oldRefs = null;
                }
            }

            void commit() {
                updateDeletionState();

                var newDataHash = _metaPart.dataHash();
                var newMetaHash = _metaPart.metaHash();
                var newExternalHash = _metaPart.externalHash();
                var newData = _dataPart.get() != null || getMeta().isHaveLocalCopy();

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

            void forceInvalidate() {
                forceInvalidate = true;
            }
        }

    }
}
