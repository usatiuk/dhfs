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
import java.util.HashMap;
import java.util.Optional;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    private final HashMap<String, NamedSoftReference> _map = new HashMap<>();
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
                synchronized (this) {
                    if (_map.containsKey(cur._key) && (_map.get(cur._key).get() == null))
                        _map.remove(cur._key);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Ref cleanup thread exiting");
    }

    private JObject<?> getFromMap(String key) {
        synchronized (this) {
            if (_map.containsKey(key)) {
                var ref = _map.get(key).get();
                if (ref != null) {
                    return ref;
                }
            }
        }
        return null;
    }

    @Override
    public Optional<JObject<?>> get(String name) {
        synchronized (this) {
            var inMap = getFromMap(name);
            if (inMap != null) return Optional.of(inMap);
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

        synchronized (this) {
            var inMap = getFromMap(name);
            if (inMap != null) {
                jObjectLRU.notifyAccess(inMap);
                return Optional.of(inMap);
            }
            JObject<?> newObj = new JObject<>(jObjectResolver, (ObjectMetadata) meta);
            _map.put(name, new NamedSoftReference(newObj, _refQueue));
            jObjectLRU.notifyAccess(newObj);
            return Optional.of(newObj);
        }
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
            synchronized (this) {
                ret = getFromMap(object.getName());
                if (ret != null) {
                    if (!object.assumeUnique())
                        throw new IllegalArgumentException("Trying to insert different object with same key");
                } else {
                    ret = new JObject<D>(jObjectResolver, object.getName(), persistentRemoteHostsService.getSelfUuid(), object);
                    _map.put(object.getName(), new NamedSoftReference(ret, _refQueue));
                    created = true;
                }
            }
            JObject<D> finalRet = (JObject<D>) ret;
            boolean finalCreated = created;
            ret.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                if (object.getClass().isAnnotationPresent(PushResolution.class) && object.assumeUnique() && finalRet.getData() == null) {
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

            synchronized (this) {
                var inMap = getFromMap(name);
                if (inMap != null) {
                    continue;
                } else {
                    // FIXME:
                    if (objectPersistentStore.existsObject("meta_" + name))
                        continue;

                    var created = new JObject<>(jObjectResolver, new ObjectMetadata(name, false, klass));
                    _map.put(name, new NamedSoftReference(created, _refQueue));
                    created.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        parent.ifPresent(m::addRef);
                        m.markSeen();
                        return null;
                    });
                    jObjectLRU.notifyAccess(created);
                    return created;
                }
            }
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
