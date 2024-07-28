package com.usatiuk.dhfs.objects.jrepository;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.stream.Stream;

@Singleton
public class JObjectResolver {
    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _writeListeners
            = new ArrayListValuedHashMap<>();
    private final MultiValuedMap<Class<? extends JObjectData>, WriteListenerFn<?>> _metaWriteListeners
            = new ArrayListValuedHashMap<>();
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
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    ProtoSerializerService protoSerializerService;
    @Inject
    JObjectRefProcessor jObjectRefProcessor;
    @Inject
    JObjectLRU jObjectLRU;
    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;

    public <T extends JObjectData> void registerWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _writeListeners.put(klass, fn);
    }

    public <T extends JObjectData> void registerMetaWriteListener(Class<T> klass, WriteListenerFn<T> fn) {
        _metaWriteListeners.put(klass, fn);
    }

    public boolean hasLocalCopy(JObject<?> self) {
        if (!self.isDeleted() && refVerification) {
            if (self.hasLocalCopyMd() && !(self.getData() != null || objectPersistentStore.existsObjectData(self.getName())))
                throw new IllegalStateException("hasLocalCopy mismatch for " + self.getName());
        }
        // FIXME: Read/write lock assert?
        return self.hasLocalCopyMd();
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
            StringBuilder sb = new StringBuilder();
            sb.append("Hydrating refs for ").append(self.getName()).append("\n");
            sb.append("Saved refs: ");
            self.getMeta().getSavedRefs().forEach(r -> sb.append(r).append(" "));
            sb.append("\nExtracted refs: ");
            var extracted = new LinkedHashSet<>(self.getData().extractRefs());
            extracted.forEach(r -> sb.append(r).append(" "));
            Log.debug(sb.toString());
            for (var r : self.getMeta().getSavedRefs()) {
                if (!extracted.contains(r))
                    jObjectManager.get(r).ifPresent(ro -> ro.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        m.removeRef(self.getName());
                        return null;
                    }));
            }
            for (var r : extracted) {
                if (!self.getMeta().getSavedRefs().contains(r)) {
                    Log.trace("Hydrating ref " + r + " for " + self.getName());
                    jObjectManager.getOrPut(r, self.getData().getRefType(), Optional.of(self.getName()));
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
                jObjectManager.getOrPut(r, self.getData().getRefType(), Optional.of(self.getName()));
            });

        }

        if (self.isDeletionCandidate() && !self.isDeleted()) {
            if (!self.getMeta().isSeen()) tryQuickDelete(self);
            else jObjectRefProcessor.putDeletionCandidate(self.getName());
        }
    }

    private void quickDeleteRef(JObject<?> self, String name) {
        jObjectManager.get(name)
                .ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
                    mc.removeRef(self.getName());
                    return null;
                }));
    }

    private void tryQuickDelete(JObject<?> self) {
        self.assertRWLock();
        if (!self.getKnownClass().isAnnotationPresent(Leaf.class))
            self.tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

        if (Log.isTraceEnabled())
            Log.trace("Quick delete of: " + self.getName());

        self.getMeta().markDeleted();

        Collection<String> extracted = null;
        if (!self.getKnownClass().isAnnotationPresent(Leaf.class) && self.getData() != null)
            extracted = self.getData().extractRefs();
        Collection<String> saved = self.getMeta().getSavedRefs();

        self.discardData();

        if (saved != null)
            for (var r : saved) quickDeleteRef(self, r);
        if (extracted != null)
            for (var r : extracted) quickDeleteRef(self, r);
    }

    public <T extends JObjectData> Optional<T> resolveDataLocal(JObject<T> jObject) {
        // jObject.assertRWLock();
        // FIXME: No way to assert read lock?
        if (objectPersistentStore.existsObjectData(jObject.getName()))
            return Optional.of(protoSerializerService.deserialize(objectPersistentStore.readObject(jObject.getName())));
        return Optional.empty();
    }

    public <T extends JObjectData> T resolveDataRemote(JObject<T> jObject) {
        var obj = remoteObjectServiceClient.getObject(jObject);
        jObjectWriteback.markDirty(jObject);
        invalidationQueueService.pushInvalidationToAll(jObject.getName());
        return protoSerializerService.deserialize(obj);
    }

    public void onResolution(JObject<?> self) {
        jObjectLRU.notifyAccess(self);
    }

    public void removeLocal(JObject<?> jObject, String name) {
        jObject.assertRWLock();
        try {
            Log.debug("Invalidating " + name);
            jObject.getMeta().setHaveLocalCopy(false);
            jObjectWriteback.remove(jObject);
            objectPersistentStore.deleteObjectData(name);
        } catch (StatusRuntimeException sx) {
            if (sx.getStatus() != Status.NOT_FOUND)
                Log.info("Couldn't delete object from persistent store: ", sx);
        } catch (Exception e) {
            Log.info("Couldn't delete object from persistent store: ", e);
        }
    }

    public <T extends JObjectData> void notifyWrite(JObject<T> self, boolean metaChanged,
                                                    boolean externalChanged, boolean hasDataChanged) {
        self.assertRWLock();
        jObjectLRU.notifyAccess(self);
        if (metaChanged || hasDataChanged)
            jObjectWriteback.markDirty(self);
        if (metaChanged)
            for (var t : _metaWriteListeners.keySet()) { // FIXME:?
                if (t.isAssignableFrom(self.getKnownClass()))
                    for (var cb : _metaWriteListeners.get(t))
                        cb.apply((JObject) self);
            }
        if (hasDataChanged)
            for (var t : _writeListeners.keySet()) { // FIXME:?
                if (t.isAssignableFrom(self.getKnownClass()))
                    for (var cb : _writeListeners.get(t))
                        cb.apply((JObject) self);
            }
        if (externalChanged) {
            invalidationQueueService.pushInvalidationToAll(self.getName());
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
                jObjectManager.get(o).ifPresent(obj -> {
                    if (obj.hasRef(self.getName()))
                        throw new IllegalStateException("Object " + o + " is referenced from " + self.getName() + " but shouldn't be");
                });
            }
        for (var r : newRefs) {
            var obj = jObjectManager.get(r).orElseThrow(() -> new IllegalStateException("Object " + r + " not found but should be referenced from " + self.getName()));
            if (obj.isDeleted())
                throw new IllegalStateException("Object " + r + " deleted but referenced from " + self.getName());
            if (!obj.hasRef(self.getName()))
                throw new IllegalStateException("Object " + r + " is not referenced by " + self.getName() + " but should be");
        }
    }

    @FunctionalInterface
    public interface WriteListenerFn<T extends JObjectData> {
        void apply(JObject<T> obj);
    }
}
