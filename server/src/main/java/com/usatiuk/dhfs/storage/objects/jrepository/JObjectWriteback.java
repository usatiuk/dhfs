package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.OrderedBidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class JObjectWriteback {

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;

    @ConfigProperty(name = "dhfs.objects.writeback.delay")
    long promotionDelay;

    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;

    @ConfigProperty(name = "dhfs.objects.writeback.nursery_limit")
    int nurseryLimit;

    @ConfigProperty(name = "dhfs.objects.writeback.threads")
    int writebackThreads;

    AtomicLong _currentSize = new AtomicLong(0);

    private final LinkedHashMap<JObject<?>, Pair<Long, Long>> _nursery = new LinkedHashMap<>();
    // FIXME: Kind of a hack
    private final OrderedBidiMap<Pair<Long, String>, JObject<?>> _writeQueue = new TreeBidiMap<>();

    private Thread _promotionThread;

    private ExecutorService _writebackExecutor;

    boolean overload = false;

    @Startup
    void init() {
        _writebackExecutor = Executors.newFixedThreadPool(writebackThreads);
        for (int i = 0; i < writebackThreads; i++) {
            _writebackExecutor.submit(this::writeback);
        }
        _promotionThread = new Thread(this::promote);
        _promotionThread.setName("Writeback promotion thread");
        _promotionThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _promotionThread.interrupt();
        while (_promotionThread.isAlive()) {
            try {
                _promotionThread.join();
            } catch (InterruptedException ignored) {
            }
        }

        _writebackExecutor.shutdownNow();

        HashSet<JObject<?>> toWrite = new LinkedHashSet<>();
        toWrite.addAll(_nursery.keySet());
        toWrite.addAll(_writeQueue.values());

        Log.info("Flushing objects");
        for (var v : toWrite) {
            try {
                flushOne(v);
            } catch (Exception e) {
                Log.error("Failed writing object " + v.getName(), e);
            }
        }
    }

    private void promote() {
        try {
            while (!Thread.interrupted()) {

                var curTime = System.currentTimeMillis();

                long wait = 0;

                synchronized (_nursery) {
                    while (_nursery.isEmpty())
                        _nursery.wait();

                    if ((curTime - _nursery.firstEntry().getValue().getLeft()) <= promotionDelay) {
                        wait = promotionDelay - (curTime - _nursery.firstEntry().getValue().getLeft());
                    }
                }

                if (wait > 0)
                    Thread.sleep(wait);

                synchronized (_nursery) {
                    while (!_nursery.isEmpty() && (curTime - _nursery.firstEntry().getValue().getLeft()) >= promotionDelay) {
                        var got = _nursery.pollFirstEntry();
                        synchronized (_writeQueue) {
                            _writeQueue.put(Pair.of(got.getValue().getRight(), got.getKey().getName()), got.getKey());
                            _writeQueue.notifyAll();
                        }
                    }
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Writeback promotion thread exiting");
    }

    private void writeback() {
        try {
            while (!Thread.interrupted()) {
                JObject<?> obj;
                long removedSize;
                synchronized (_writeQueue) {
                    while (_writeQueue.isEmpty())
                        _writeQueue.wait();

                    var fk = _writeQueue.lastKey();
                    removedSize = fk.getKey();
                    obj = _writeQueue.remove(fk);
                }
                try {
                    _currentSize.addAndGet(-removedSize);
                    flushOne(obj);
                } catch (Exception e) {
                    Log.error("Failed writing object " + obj.getName() + ", will retry.", e);
                    try {
                        obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                            var size = jObjectSizeEstimator.estimateObjectSize(d);
                            synchronized (_writeQueue) {
                                _writeQueue.put(Pair.of(size, m.getName()), obj);
                            }
                            return null;
                        });
                    } catch (JObject.DeletedObjectAccessException ignored) {
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
        m.markWritten();
        objectPersistentStore.writeObject("meta_" + m.getName(), SerializationHelper.serialize(m));
        if (data != null)
            objectPersistentStore.writeObject(m.getName(), SerializationHelper.serialize(data));
    }

    public void remove(JObject<?> object) {
        object.assertRWLock();
        synchronized (_nursery) {
            if (_nursery.containsKey(object)) {
                var size = _nursery.get(object).getRight();
                _nursery.remove(object);
                _currentSize.addAndGet(-size);
            }
        }
        synchronized (_writeQueue) {
            if (_writeQueue.containsValue(object)) {
                var size = _writeQueue.inverseBidiMap().get(object).getLeft();
                _writeQueue.removeValue(object);
                _currentSize.addAndGet(-size);
            }
        }
    }

    public void markDirty(JObject<?> object) {
        object.assertRWLock();
        if (object.isDeleted() && !object.getMeta().isWritten()) {
            remove(object);
            return;
        }

        var size = jObjectSizeEstimator.estimateObjectSize(object.getData());

        synchronized (_nursery) {
            if (_nursery.containsKey(object)) {
                long oldSize = _nursery.get(object).getRight();
                if (oldSize == size)
                    return;
                long oldTime = _nursery.get(object).getLeft();
                if (nurseryLimit > 0 && size >= nurseryLimit) {
                    _nursery.remove(object);
                    _currentSize.addAndGet(-oldSize);
                } else {
                    _nursery.replace(object, Pair.of(oldTime, size));
                    _currentSize.addAndGet(size - oldSize);
                    return;
                }
            }
        }

        synchronized (_writeQueue) {
            if (_writeQueue.containsValue(object)) {
                long oldSize = _writeQueue.getKey(object).getKey();
                if (oldSize == size)
                    return;
                _currentSize.addAndGet(size - oldSize);
                _writeQueue.inverseBidiMap().replace(object, Pair.of(size, object.getName()));
                return;
            }
        }

        var curTime = System.currentTimeMillis();

        if (nurseryLimit > 0 && size >= nurseryLimit) {
            synchronized (_writeQueue) {
                _currentSize.addAndGet(size);
                _writeQueue.put(Pair.of(size, object.getName()), object);
                _writeQueue.notifyAll();
                return;
            }
        }

        synchronized (_nursery) {
            if (_currentSize.get() < sizeLimit) {
                if (overload) {
                    overload = false;
                    Log.trace("Writeback cache enabled");
                }
                _nursery.put(object, Pair.of(curTime, size));
                _currentSize.addAndGet(size);
                _nursery.notifyAll();
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
            throw e;
        }
    }
}
