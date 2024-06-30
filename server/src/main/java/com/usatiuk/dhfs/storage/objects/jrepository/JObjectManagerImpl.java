package com.usatiuk.dhfs.storage.objects.jrepository;

import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
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
import java.util.stream.Stream;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    JObjectResolver jObjectResolver;

    @Inject
    JObjectWriteback jObjectWriteback;

    @Inject
    JObjectRefProcessor jobjectRefProcessor;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    private static class NamedSoftReference extends SoftReference<JObject<?>> {
        public NamedSoftReference(JObject<?> target, ReferenceQueue<JObject<?>> q) {
            super(target, q);
            this._key = target.getName();
        }

        @Getter
        final String _key;
    }

    private final HashMap<String, NamedSoftReference> _map = new HashMap<>();
    private final ReferenceQueue<JObject<?>> _refQueue = new ReferenceQueue<>();

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

        ByteString readMd;
        try {
            readMd = objectPersistentStore.readObject("meta_" + name);
        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode().equals(Status.NOT_FOUND.getCode()))
                return Optional.empty();
            throw ex;
        }
        var meta = SerializationHelper.deserialize(readMd);
        if (!(meta instanceof ObjectMetadata))
            throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Unexpected metadata type for " + name));

        if (((ObjectMetadata) meta).isDeleted()) {
            Log.warn("Deleted meta on disk for " + name);
            return Optional.empty();
        }

        synchronized (this) {
            var inMap = getFromMap(name);
            if (inMap != null) return Optional.of(inMap);
            JObject<?> newObj = new JObject<>(jObjectResolver, (ObjectMetadata) meta);
            _map.put(name, new NamedSoftReference(newObj, _refQueue));
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
            synchronized (this) {
                ret = getFromMap(object.getName());
                if (ret != null) {
                    if (!object.assumeUnique())
                        throw new IllegalArgumentException("Trying to insert different object with same key");
                } else {
                    ret = new JObject<D>(jObjectResolver, object.getName(), persistentRemoteHostsService.getSelfUuid(), object);
                    _map.put(object.getName(), new NamedSoftReference(ret, _refQueue));
                }
            }
            ret.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                if (parent.isPresent()) {
                    m.addRef(parent.get());
                } else {
                    m.lock();
                }
                return true;
            });
            return (JObject<D>) ret;
        }
    }

    @Override
    public JObject<?> getOrPut(String name, Optional<String> parent) {
        while (true) {
            var got = get(name);

            if (got.isPresent()) {
                if (parent.isPresent())
                    got.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        m.addRef(parent.get());
                        return true;
                    });
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

                    var created = new JObject<>(jObjectResolver, new ObjectMetadata(name, false));
                    _map.put(name, new NamedSoftReference(created, _refQueue));
                    created.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        parent.ifPresent(m::addRef);
                        m.markSeen();
                        return null;
                    });
                    return created;
                }
            }
        }
    }

    @Override
    public void tryQuickDelete(JObject<?> object) {
        object.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
            if (m.getRefcount() > 0) return false;
            if (m.isSeen()) {
                jobjectRefProcessor.putDeletionCandidate(object.getName());
                return false;
            }

            object.tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

            Log.trace("Quick delete of " + m.getName());
            m.delete();

            Stream<String> refs = Stream.empty();

            if (!m.getSavedRefs().isEmpty())
                refs = m.getSavedRefs().stream();
            if (object.getData() != null)
                refs = Streams.concat(refs, object.getData().extractRefs().stream());

            object.discardData();

            refs.forEach(c -> get(c).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
                mc.removeRef(object.getName());
                tryQuickDelete(ref);
                return null;
            })));

            return true;
        });
    }

    @Override
    public void notifySent(String key) {
        //FIXME:
        get(key).ifPresent(o -> o.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
            m.markSeen();
            return null;
        }));
    }
}
