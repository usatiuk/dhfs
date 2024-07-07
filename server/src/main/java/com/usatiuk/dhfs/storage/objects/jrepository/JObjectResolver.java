package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.InvalidationQueueService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;

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

    private final MultiValuedMap<Class<? extends JObjectData>, JObject.ObjectFnWrite<?, Void>> _writeListeners
            = new ArrayListValuedHashMap<>();

    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;

    @ConfigProperty(name = "dhfs.objects.bump_verification")
    boolean bumpVerification;

    public <T extends JObjectData> void registerWriteListener(Class<T> klass, JObject.ObjectFnWrite<T, Void> fn) {
        _writeListeners.put(klass, fn);
    }

    public void backupRefs(JObject<?> self) {
        self.assertRWLock();
        if (self.getData() != null) {
            if ((self.getMeta().getSavedRefs() != null) && (!self.getMeta().getSavedRefs().isEmpty())) {
                Log.error("Saved refs not empty for " + self.getName() + " will clean");
                self.getMeta().setSavedRefs(null);
            }
            self.getMeta().setSavedRefs(new LinkedHashSet<>(self.getData().extractRefs()));
        } else {
            self.getMeta().setSavedRefs(Collections.emptySet());
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

        if (self.getMeta().getRefcount() > 0) {
            if (self.isDeleted()) {
                self.getMeta().undelete();
                if (self.isResolved()) {
                    for (var r : self.getData().extractRefs()) {
                        Log.trace("Hydrating ref after undelete " + r + " for " + self.getName());
                        jobjectManager.getOrPut(r, self.getData().getRefType(), Optional.of(self.getName()));
                    }
                }
            }
        }

        if (self.getMeta().getRefcount() <= 0)
            if (!self.isDeleted())
                jObjectRefProcessor.putDeletionCandidate(self.getName());
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

    public void notifyWriteMeta(JObject<?> self) {
        self.assertRWLock();
        jObjectWriteback.markDirty(self);
    }

    public <T extends JObjectData> void notifyWriteData(JObject<T> self) {
        self.assertRWLock();
        jObjectWriteback.markDirty(self);
        if (self.isResolved()) {
            invalidationQueueService.pushInvalidationToAll(self.getName());
            for (var l : _writeListeners.get(self.getData().getClass())) {
                // TODO: Assert types?
                self.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (JObject.ObjectFnWrite<T, ?>) l);
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

    protected void verifyRefs(JObject<?> self) {
        if (!refVerification) return;
        self.assertRWLock();
        if (!self.isResolved()) return;
        if (self.isDeleted()) return;
        for (var r : self.getData().extractRefs()) {
            var obj = jobjectManager.get(r).orElseThrow(() -> new IllegalStateException("Object " + r + " not found but should be referenced from " + self.getName()));
            if (obj.isDeleted())
                throw new IllegalStateException("Object " + r + " deleted but referenced from " + self.getName());
            obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                if (!m.checkRef(self.getName()))
                    throw new IllegalStateException("Object " + r + " is not referenced by " + self.getName() + " but should be");
                return null;
            });
        }
    }
}
