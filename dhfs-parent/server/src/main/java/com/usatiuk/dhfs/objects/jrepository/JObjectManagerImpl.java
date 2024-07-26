package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.BlobP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Getter;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    private final ConcurrentHashMap<String, NamedSoftReference> _map = new ConcurrentHashMap<>();
    private final ReferenceQueue<JObject<?>> _refQueue = new ReferenceQueue<>();
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    JObjectResolver jObjectResolver;
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    ProtoSerializerService protoSerializerService;
    @Inject
    JObjectLRU jObjectLRU;

    private Thread _refCleanupThread;

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
                NamedSoftReference cur = (NamedSoftReference) _refQueue.remove();
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
    public Optional<JObject<?>> get(String name) {
        {
            var inMap = getFromMap(name);
            if (inMap != null) {
                jObjectLRU.notifyAccess(inMap);
                return Optional.of(inMap);
            }
        }

        BlobP readMd;
        try {
            readMd = objectPersistentStore.readObject("meta_" + name);
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
        var newObj = new JObject<>(jObjectResolver, (ObjectMetadata) meta);
        while (ret == null) {
            var ref = _map.computeIfAbsent(name, k -> new NamedSoftReference(newObj, _refQueue));
            if (ref.get() == null) _map.remove(name, ref);
            else ret = ref.get();
        }
        jObjectLRU.notifyAccess(ret);
        return Optional.of(ret);
    }

    @Override
    public Collection<JObject<?>> find(String prefix) {
        var ret = new ArrayList<JObject<?>>();
        for (var f : objectPersistentStore.findObjects("meta_")) {
            var got = get(f.substring(5));
            if (got.isPresent())
                ret.add(got.get());
        }
        return ret;
    }

    @Override
    public <D extends JObjectData> JObject<D> put(D object, Optional<String> parent) {
        while (true) {
            JObject<?> ret;
            boolean created = false;
            JObject<?> newObj = null;
            try {
                ret = getFromMap(object.getName());
                if (ret != null) {
                    if (!object.getClass().isAnnotationPresent(AssumedUnique.class))
                        throw new IllegalArgumentException("Trying to insert different object with same key");
                } else {
                    newObj = new JObject<D>(jObjectResolver, object.getName(), persistentRemoteHostsService.getSelfUuid(), object);
                    newObj.rwLock();
                    while (ret == null) {
                        JObject<?> finalNewObj = newObj;
                        var ref = _map.computeIfAbsent(object.getName(), k -> new NamedSoftReference(finalNewObj, _refQueue));
                        if (ref.get() == null) _map.remove(object.getName(), ref);
                        else ret = ref.get();
                    }
                    if (ret != newObj) continue;
                    created = true;
                }
                JObject<D> finalRet = (JObject<D>) ret;
                boolean finalCreated = created;
                ret.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
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

                    if (finalCreated) finalRet.notifyWrite();// Kind of a hack?
                    return null;
                });
            } finally {
                if (newObj != null) newObj.rwUnlock();
            }
            if (!created)
                jObjectLRU.notifyAccess(ret);
            return (JObject<D>) ret;
        }
    }

    @Override
    public JObject<?> getOrPut(String name, Class<? extends JObjectData> klass, Optional<String> parent) {
        while (true) {
            var got = get(name);

            if (got.isPresent()) {
                got.get().narrowClass(klass);
                got.get().markSeen();
                parent.ifPresent(s -> got.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    if (m.isLocked())
                        m.unlock();
                    m.addRef(s);
                    return true;
                }));
                jObjectLRU.notifyAccess(got.get());
                return got.get();
            }

            JObject<?> ret = null;
            var created = new JObject<>(jObjectResolver, new ObjectMetadata(name, false, klass));
            created.rwLock();
            try {
                while (ret == null) {
                    var ref = _map.computeIfAbsent(name, k -> new NamedSoftReference(created, _refQueue));
                    if (ref.get() == null) _map.remove(name, ref);
                    else ret = ref.get();
                }
                if (ret != created) continue;

                created.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    parent.ifPresent(m::addRef);
                    m.markSeen();
                    return null;
                });
            } finally {
                created.rwUnlock();
            }
            return created;
        }
    }

    private static class NamedSoftReference extends SoftReference<JObject<?>> {
        @Getter
        final String _key;

        public NamedSoftReference(JObject<?> target, ReferenceQueue<JObject<?>> q) {
            super(target, q);
            this._key = target.getName();
        }
    }
}
