package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

@ApplicationScoped
public class JObjectWriteback {

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    JObjectManager jObjectManager;

    private final LinkedHashMap<String, Pair<Long, JObject<?>>> _objects = new LinkedHashMap<>();
    private final LinkedHashSet<String> _toIgnore = new LinkedHashSet<>();

    private Thread _writebackThread;

    @Startup
    void init() {
        _writebackThread = new Thread(this::writeback);
        _writebackThread.setName("JObject writeback thread");
        _writebackThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _writebackThread.interrupt();
        while (_writebackThread.isAlive()) {
            try {
                _writebackThread.join();
            } catch (InterruptedException ignored) {
            }
        }

        Collection<Pair<Long, JObject<?>>> toWrite;
        synchronized (_objects) {
            toWrite = new ArrayList<>(_objects.values());
        }
        for (var v : toWrite) {
            flushOne(v.getRight());
        }
    }

    private void writeback() {
        try {
            boolean wait = false;
            while (true) {
                if (wait) {
                    Thread.sleep(500);
                    wait = false;
                }
                JObject<?> obj;
                synchronized (_objects) {
                    while (_objects.isEmpty())
                        _objects.wait();

                    if ((System.currentTimeMillis() - _objects.firstEntry().getValue().getLeft()) < 500L) {
                        wait = true;
                        continue;
                    }

                    var entry = _objects.pollFirstEntry();
                    if (entry == null) break;
                    obj = entry.getValue().getRight();
                }
                flushOne(obj);
                if (Thread.interrupted()) break;
            }
        } catch (InterruptedException e) {
            Log.info("Writeback thread exiting");
        }
    }

    private void flushOne(JObject<?> obj) {
        obj.runReadLocked((m) -> {
            objectPersistentStore.writeObject("meta_" + m.getName(), SerializationHelper.serialize(m));
            if (obj.isResolved())
                obj.runReadLocked((m2, d) -> {
                    objectPersistentStore.writeObject(m.getName(), SerializationHelper.serialize(d));
                    return null;
                });
            jObjectManager.onWriteback(m.getName());
            return null;
        });
    }

    private void flushOneImmediate(JObject<?> obj) {
        obj.runWriteLockedMeta((m, a, b) -> {
            objectPersistentStore.writeObject("meta_" + m.getName(), SerializationHelper.serialize(m));
            if (obj.isResolved())
                obj.runWriteLocked((m2, d, bump) -> {
                    objectPersistentStore.writeObject(m.getName(), SerializationHelper.serialize(d));
                    return null;
                });
            jObjectManager.onWriteback(m.getName());
            return null;
        });
    }

    public void remove(String name) {
        synchronized (_objects) {
            _objects.remove(name);
        }
    }

    public void markDirty(String name, JObject<?> object) {
        synchronized (_objects) {
            if (_objects.containsKey(name)) {
                return;
            }

            // FIXME: better logic
            if (_objects.size() < 10000) {
                _objects.put(name, Pair.of(System.currentTimeMillis(), object));
                _objects.notifyAll();
                return;
            }
        }
        flushOneImmediate(object);
    }
}
