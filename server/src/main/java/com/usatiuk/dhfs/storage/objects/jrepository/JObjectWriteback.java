package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;

@ApplicationScoped
public class JObjectWriteback {

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.objects.writeback.delay")
    Integer delay;

    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    Integer limit;

    private final LinkedHashMap<String, Pair<Long, JObject<?>>> _objects = new LinkedHashMap<>();

    private Thread _writebackThread;

    boolean overload = false;

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
            try {
                flushOne(v.getRight());
            } catch (Exception e) {
                Log.error("Failed writing object " + v.getRight().getName(), e);
            }
        }
    }

    private void writeback() {
        try {
            boolean wait = false;
            while (!Thread.interrupted()) {
                if (wait) {
                    Thread.sleep(delay);
                    wait = false;
                }
                JObject<?> obj;
                synchronized (_objects) {
                    while (_objects.isEmpty())
                        _objects.wait();

                    if ((System.currentTimeMillis() - _objects.firstEntry().getValue().getLeft()) < delay) {
                        wait = true;
                        continue;
                    }

                    var entry = _objects.pollFirstEntry();
                    if (entry == null) break;
                    obj = entry.getValue().getRight();
                }
                try {
                    flushOne(obj);
                } catch (Exception e) {
                    Log.error("Failed writing object " + obj.getName(), e);
                    synchronized (_objects) {
                        _objects.put(obj.getName(), Pair.of(System.currentTimeMillis(), obj));
                    }
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Writeback thread exiting");
    }

    private void flushOne(JObject<?> obj) {
        if (obj.isDeleted())
            obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, data, b, i) -> {
                flushOneImmediate(m, data);
                return null;
            });
        else
            obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, data) -> {
                flushOneImmediate(m, data);
                return null;
            });
    }

    private <T extends JObjectData> void flushOneImmediate(ObjectMetadata m, T data) {
        if (m.isDeleted()) {
            if (m.getRefcount() > 0)
                throw new IllegalStateException("Object deleted but has refcounts! " + m.getName());
            // FIXME: assert Rw lock here?
            Log.trace("Deleting from persistent storage " + m.getName());
            objectPersistentStore.deleteObject("meta_" + m.getName());
            objectPersistentStore.deleteObject(m.getName());
            return;
        }
        objectPersistentStore.writeObject("meta_" + m.getName(), SerializationHelper.serialize(m));
        if (data != null)
            objectPersistentStore.writeObject(m.getName(), SerializationHelper.serialize(data));
    }

    public void remove(String name) {
        synchronized (_objects) {
            _objects.remove(name);
        }
    }

    private void tryClean() {
        synchronized (_objects) {
            if (_objects.size() >= limit) {
                for (var obj : _objects.entrySet()) {
                    var jobj = obj.getValue().getValue();
                    if (jobj.isDeleted()) {
                        if (jobj.tryRwLock()) {
                            try {
                                if (!jobj.isDeleted())
                                    continue;
                                flushOneImmediate(jobj.getMeta(), null);
                                _objects.remove(jobj.getName());
                                break;
                            } finally {
                                obj.getValue().getRight().rwUnlock();
                            }
                        }
                    }
                }
            }
        }
    }

    public void markDirty(String name, JObject<?> object) {
        object.assertRWLock();
        synchronized (_objects) {
            if (_objects.containsKey(name)) {
                return;
            }

            tryClean();

            // FIXME: better logic
            if (_objects.size() < limit) {
                if (overload) {
                    overload = false;
                    Log.trace("Writeback cache enabled");
                }
                _objects.put(name, Pair.of(System.currentTimeMillis(), object));
                _objects.notifyAll();
                return;
            }
        }

        try {
            if (!overload) {
                overload = true;
                Log.trace("Writeback cache disabled");
            }
            flushOneImmediate(object.getMeta(), object.getData());
        } catch (Exception e) {
            Log.error("Failed writing object " + object.getName(), e);
            synchronized (_objects) {
                _objects.put(object.getName(), Pair.of(System.currentTimeMillis(), object));
            }
        }
    }
}
