package com.usatiuk.dhfs.objects.jrepository;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.repository.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.stream.Stream;

@Singleton
public class JObjectResolver {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    InvalidationQueueService invalidationQueueService;

    @Inject
    JObjectWriteback jObjectWriteback;

    @Inject
    JObjectManager jobjectManager;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    JObjectRefProcessor jObjectRefProcessor;

    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _writeListeners
            = new ArrayListValuedHashMap<>();
    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _metaWriteListeners
            = new ArrayListValuedHashMap<>();

    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;

    @ConfigProperty(name = "dhfs.objects.bump_verification")
    boolean bumpVerification;


    @FunctionalInterface
    public interface WriteListenerFn<T extends JObjectData> {
        void apply(JObject<T> obj);
    }

    public <T extends JObjectData> void registerWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _writeListeners.put(klass, fn);
    }

    public <T extends JObjectData> void registerMetaWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _metaWriteListeners.put(klass, fn);
    }

    public boolean hasLocalCopy(JObject<?> self) {
        // FIXME: Read/write lock assert?
        return objectPersistentStore.existsObject(self.getName());
    }

    public void backupRefs(JObject<?> self) {
        self.assertRWLock();
        if (self.getData() != null) {
            if ((self.getMeta().getSavedRefs() != null) && (!self.getMeta().getSavedRefs().isEmpty())) {
                Log.error("Saved refs not empty for " + self.getName() + " will clean");
                self.getMeta().setSavedRefs(null);
            }
            self.getMeta().setSavedRefs(new LinkedHashSet<>(self.getData().extractRefs()));
        }
    }

    public void hydrateRefs(JObject<?> self) {
        self.assertRWLock();
        if (self.getMeta().getSavedRefs() != null) {
            var extracted = new LinkedHashSet<>(self.getData().extractRefs());
            for (var r : self.getMeta().getSavedRefs()) {
                if (!extracted.contains(r))
                    jobjectManager.get(r).ifPresent(ro -> ro.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        m.removeRef(self.getName());
                        return null;
                    }));
            }
            for (var r : extracted) {
                if (!self.getMeta().getSavedRefs().contains(r)) {
                    Log.trace("Hydrating ref " + r + " for " + self.getName());
                    jobjectManager.getOrPut(r, self.getData().getRefType(), Optional.of(self.getName()));
                }
            }
            self.getMeta().setSavedRefs(null);
        }
    }

    public void updateDeletionState(JObject<?> self) {
        self.assertRWLock();

        if (!self.isDeletionCandidate() && self.isDeleted()) {
            self.getMeta().undelete();
            Log.debug("Undelete: " + self.getName());

            Stream<String> refs = Stream.empty();

            if (self.getMeta().getSavedRefs() != null)
                refs = self.getMeta().getSavedRefs().stream();
            if (self.getData() != null)
                refs = Streams.concat(refs, self.getData().extractRefs().stream());

            refs.forEach(r -> {
                Log.trace("Hydrating ref after undelete " + r + " for " + self.getName());
                jobjectManager.getOrPut(r, self.getData().getRefType(), Optional.of(self.getName()));
            });

        }

        if (self.isDeletionCandidate() && !self.isDeleted()) {
            jObjectRefProcessor.putDeletionCandidate(self.getName());
        }
    }

    public <T extends JObjectData> Optional<T> resolveDataLocal(JObject<T> jObject) {
        // jObject.assertRWLock();
        // FIXME: No way to assert read lock?
        if (objectPersistentStore.existsObject(jObject.getName()))
            return Optional.of(SerializationHelper.deserialize(objectPersistentStore.readObject(jObject.getName())));
        return Optional.empty();
    }

    public <T extends JObjectData> T resolveData(JObject<T> jObject) {
        jObject.assertRWLock();
        var local = resolveDataLocal(jObject);
        if (local.isPresent()) return local.get();

        var obj = remoteObjectServiceClient.getObject(jObject);
        jObjectWriteback.markDirty(jObject);
        invalidationQueueService.pushInvalidationToAll(jObject.getName());
        return SerializationHelper.deserialize(obj);
    }

    public void removeLocal(JObject<?> jObject, String name) {
        jObject.assertRWLock();
        try {
            Log.trace("Invalidating " + name);
            jObjectWriteback.remove(jObject);
            objectPersistentStore.deleteObject(name);
        } catch (StatusRuntimeException sx) {
            if (sx.getStatus() != Status.NOT_FOUND)
                Log.info("Couldn't delete object from persistent store: ", sx);
        } catch (Exception e) {
            Log.info("Couldn't delete object from persistent store: ", e);
        }
    }

    public <T extends JObjectData> void notifyWriteMeta(JObject<T> self) {
        self.assertRWLock();
        jObjectWriteback.markDirty(self);
        for (var t : _metaWriteListeners.keySet()) { // FIXME:?
            if (t.isAssignableFrom(self.getKnownClass()))
                for (var cb : _metaWriteListeners.get(t))
                    cb.apply((JObject) self);
        }
    }

    public <T extends JObjectData> void notifyWriteData(JObject<T> self) {
        self.assertRWLock();
        jObjectWriteback.markDirty(self);
        if (self.isResolved()) {
            invalidationQueueService.pushInvalidationToAll(self.getName());
            for (var t : _writeListeners.keySet()) { // FIXME:?
                if (t.isAssignableFrom(self.getKnownClass()))
                    for (var cb : _writeListeners.get(t))
                        cb.apply((JObject) self);
            }
        }
    }

    public void bumpVersionSelf(JObject<?> self) {
        self.assertRWLock();
        self.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, data, bump, invalidate) -> {
            m.bumpVersion(persistentRemoteHostsService.getSelfUuid());
            return null;
        });
    }

    protected void verifyRefs(JObject<?> self, @Nullable HashSet<String> oldRefs) {
        if (!refVerification) return;
        self.assertRWLock();
        if (!self.isResolved()) return;
        if (self.isDeleted()) return;
        var newRefs = self.getData().extractRefs();
        if (oldRefs != null) for (var o : oldRefs)
            if (!newRefs.contains(o)) {
                jobjectManager.get(o).ifPresent(obj -> {
                    if (obj.hasRef(self.getName()))
                        throw new IllegalStateException("Object " + o + " is referenced from " + self.getName() + " but shouldn't be");
                });
            }
        for (var r : newRefs) {
            var obj = jobjectManager.get(r).orElseThrow(() -> new IllegalStateException("Object " + r + " not found but should be referenced from " + self.getName()));
            if (obj.isDeleted())
                throw new IllegalStateException("Object " + r + " deleted but referenced from " + self.getName());
            if (!obj.hasRef(self.getName()))
                throw new IllegalStateException("Object " + r + " is not referenced by " + self.getName() + " but should be");
        }
    }
}
