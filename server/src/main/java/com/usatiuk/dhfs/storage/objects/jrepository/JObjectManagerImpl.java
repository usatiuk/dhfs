package com.usatiuk.dhfs.storage.objects.jrepository;

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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    JObjectResolver jObjectResolver;

    @Inject
    JObjectWriteback jObjectWriteback;

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
    private final HashMap<String, Long> _nurseryRefcounts = new HashMap<>();
    private final ReferenceQueue<JObject<?>> _refQueue = new ReferenceQueue<>();
    private final AtomicReference<LinkedHashSet<String>> _writebackQueue = new AtomicReference<>(new LinkedHashSet<>());

    private Thread _refCleanupThread;
    private Thread _nurseryCleanupThread;

    @Startup
    void init() {
        _refCleanupThread = new Thread(this::refCleanupThread);
        _refCleanupThread.setName("JObject ref cleanup thread");
        _refCleanupThread.start();
        _nurseryCleanupThread = new Thread(this::nurseryCleanupThread);
        _nurseryCleanupThread.setName("JObject nursery cleanup thread");
        _nurseryCleanupThread.start();
    }

    @Shutdown
    void shutdown() throws InterruptedException {
        _refCleanupThread.interrupt();
        _nurseryCleanupThread.interrupt();
        _refCleanupThread.join();
        _nurseryCleanupThread.join();
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

    private void nurseryCleanupThread() {
        try {
            while (!Thread.interrupted()) {
                LinkedHashSet<String> got;

                synchronized (_writebackQueue) {
                    while (_writebackQueue.get().isEmpty())
                        _writebackQueue.wait();
                    got = _writebackQueue.get();
                    _writebackQueue.set(new LinkedHashSet<>());
                }

                synchronized (this) {
                    for (var s : got) {
                        _nurseryRefcounts.remove(s);
                    }
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

        synchronized (this) {
            var inMap = getFromMap(name);
            if (inMap != null) return Optional.of(inMap);
            JObject<?> newObj = new JObject<>(jObjectResolver, (ObjectMetadata) meta);
            _map.put(name, new NamedSoftReference(newObj, _refQueue));
            return Optional.of(newObj);
        }
    }

    @Override
    public <D extends JObjectData> Optional<JObject<? extends D>> get(String name, Class<D> klass) {
        var got = get(name);
        if (got.isEmpty()) return Optional.empty();
        if (!got.get().isOf(klass))
            throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Class mismatch for " + name));
        return Optional.of((JObject<? extends D>) got.get());
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
    public <D extends JObjectData> JObject<D> put(D object) {
        synchronized (this) {
            var inMap = getFromMap(object.getName());
            if (inMap != null) {
                if (!object.assumeUnique())
                    throw new IllegalArgumentException("Trying to insert different object with same key");
                addToNursery(object.getName());
                return (JObject<D>) inMap;
            } else {
                var created = new JObject<D>(jObjectResolver, object.getName(),
                        object.getConflictResolver().getName(), persistentRemoteHostsService.getSelfUuid(), object);
                _map.put(object.getName(), new NamedSoftReference(created, _refQueue));
                created.runWriteLockedMeta((m, d, b) -> {
                    jObjectResolver.notifyWrite(created);
                    return null;
                });
                addToNursery(created.getName());
                return created;
            }
        }
    }

    @Override
    public JObject<?> getOrPut(String name, ObjectMetadata md) {
        var got = get(name);

        if (got.isPresent()) {
            if (!got.get().isOf(md.getType())) {
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Type mismatch for " + name));
            }
            return got.get();
        }

        synchronized (this) {
            var inMap = getFromMap(md.getName());
            if (inMap != null) {
                return inMap;
            } else {
                var created = new JObject<>(jObjectResolver, md);
                _map.put(md.getName(), new NamedSoftReference(created, _refQueue));
                created.runWriteLockedMeta((m, d, b) -> {
                    jObjectResolver.notifyWrite(created);
                    return null;
                });
                return created;
            }
        }
    }

    private void addToNursery(String name) {
        synchronized (this) {
            if (!objectPersistentStore.existsObject("meta_" + name))
                _nurseryRefcounts.merge(name, 1L, Long::sum);
        }
    }

    @Override
    public void onWriteback(String name) {
        synchronized (_writebackQueue) {
            _writebackQueue.get().add(name);
            _writebackQueue.notifyAll();
        }
    }

    @Override
    public void unref(JObject<?> object) {
        object.runWriteLockedMeta((m, a, b) -> {
            String name = m.getName();
            synchronized (this) {
                if (!_nurseryRefcounts.containsKey(name)) return null;

                if (objectPersistentStore.existsObject("meta_" + name))
                    _nurseryRefcounts.remove(name);

                _nurseryRefcounts.merge(name, -1L, Long::sum);
                if (_nurseryRefcounts.get(name) <= 0) {
                    _nurseryRefcounts.remove(name);
                    jObjectWriteback.remove(name);
                    _map.remove(name);
                }
            }
            return null;
        });
    }
}
