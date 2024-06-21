package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;

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
            while (true) {
                NamedSoftReference cur = (NamedSoftReference) _refQueue.remove();
                synchronized (this) {
                    if (_map.containsKey(cur._key) && (_map.get(cur._key).get() == null))
                        _map.remove(cur._key);
                }
            }
        } catch (InterruptedException ignored) {
            Log.info("Ref cleanup thread exiting");
        }
    }

    private void nurseryCleanupThread() {
        try {
            LinkedHashSet<String> got;

            synchronized (_writebackQueue) {
                _writebackQueue.wait();
                got = _writebackQueue.get();
                _writebackQueue.set(new LinkedHashSet<>());
            }

            synchronized (this) {
                for (var s : got) {
                    _nurseryRefcounts.remove(s);
                }
            }
        } catch (InterruptedException ignored) {
            Log.info("Ref cleanup thread exiting");
        }
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

        byte[] readMd;
        try {
            readMd = objectPersistentStore.readObject("meta_" + name);
        } catch (StatusRuntimeException ex) {
            if (!ex.getStatus().equals(Status.NOT_FOUND)) throw ex;
            return Optional.empty();
        }
        var meta = DeserializationHelper.deserialize(readMd);
        if (!(meta instanceof ObjectMetadata))
            throw new NotImplementedException("Unexpected metadata type for " + name);

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
        if (!got.get().isOf(klass)) throw new NotImplementedException("Class mismatch for " + name);
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
                var created = new JObject<D>(jObjectResolver, object.getName(), object.getConflictResolver().getName(), object);
                _map.put(object.getName(), new NamedSoftReference(created, _refQueue));
                jObjectResolver.notifyWrite(created);
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
                throw new NotImplementedException("Type mismatch for " + name);
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
                jObjectResolver.notifyWrite(created);
                return created;
            }
        }
    }

    @Override
    public <D extends JObjectData> JObject<D> getOrPut(String name, D object) {
        var got = get(name);
        if (got.isPresent()) {
            if (!got.get().isOf(object.getClass())) {
                throw new NotImplementedException("Type mismatch for " + name);
            }
            return (JObject<D>) got.get();
        }

        synchronized (this) {
            var inMap = getFromMap(object.getName());
            if (inMap != null) {
                if (inMap.isOf(object.getClass()))
                    return (JObject<D>) inMap;
                else
                    throw new NotImplementedException("Type mismatch for " + name);
            } else {
                var created = new JObject<D>(jObjectResolver, object.getName(), object.getConflictResolver().getName(), object);
                _map.put(object.getName(), new NamedSoftReference(created, _refQueue));
                jObjectResolver.notifyWrite(created);
                return created;
            }
        }
    }

    private void addToNursery(String name) {
        synchronized (this) {
            if (!objectPersistentStore.existsObject("meta_" + name))
                _nurseryRefcounts.merge(name, 1L, Long::sum);
            else
                _nurseryRefcounts.remove(name);
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
                if (objectPersistentStore.existsObject("meta_" + name)) {
                    _nurseryRefcounts.remove(name);
                    return null;
                }

                if (!_nurseryRefcounts.containsKey(name)) return null;
                _nurseryRefcounts.merge(name, -1L, Long::sum);
                if (_nurseryRefcounts.get(name) <= 0) {
                    _nurseryRefcounts.remove(name);
                    jObjectWriteback.remove(name);
                    if (!objectPersistentStore.existsObject("meta_" + name))
                        _map.remove(name);
                }
            }
            return null;
        });
    }
}
