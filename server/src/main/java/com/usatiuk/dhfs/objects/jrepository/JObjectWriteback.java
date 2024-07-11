package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class JObjectWriteback {
    private class QueueEntry {
        private final JObject<?> _obj;
        private long _size;

        private QueueEntry(JObject<?> obj, long size) {
            _obj = obj;
            _size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueueEntry that = (QueueEntry) o;
            return Objects.equals(_obj, that._obj);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_obj);
        }
    }

    private final HashSetDelayedBlockingQueue<QueueEntry> _writeQueue;
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    @ConfigProperty(name = "dhfs.objects.writeback.watermark-high")
    float watermarkHighRatio;
    @ConfigProperty(name = "dhfs.objects.writeback.watermark-low")
    float watermarkLowRatio;
    @ConfigProperty(name = "dhfs.objects.writeback.threads")
    int writebackThreads;

    private final AtomicLong _currentSize = new AtomicLong(0);
    private final AtomicBoolean _watermarkReached = new AtomicBoolean(false);
    private final AtomicBoolean _shutdown = new AtomicBoolean(false);

    private ExecutorService _writebackExecutor;
    private ExecutorService _statusExecutor;

    public JObjectWriteback(@ConfigProperty(name = "dhfs.objects.writeback.delay") long promotionDelay) {
        _writeQueue = new HashSetDelayedBlockingQueue<>(promotionDelay);
    }

    @Startup
    void init() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("writeback-%d")
                .build();

        _writebackExecutor = Executors.newFixedThreadPool(writebackThreads, factory);
        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    Log.info("Writeback status: size="
                            + _currentSize.get() / 1024 / 1024 + "MB"
                            + " watermark=" + (_watermarkReached.get() ? "reached" : "not reached"));
                }
            } catch (InterruptedException ignored) {
            }
        });
        for (int i = 0; i < writebackThreads; i++) {
            _writebackExecutor.submit(this::writeback);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _shutdown.set(true);
        _writebackExecutor.shutdownNow();
        _statusExecutor.shutdownNow();

        var toWrite = _writeQueue.close();

        Log.info("Flushing objects");
        for (var v : toWrite) {
            try {
                flushOne(v._obj);
            } catch (Exception e) {
                Log.error("Failed writing object " + v._obj.getName(), e);
            }
        }
    }

    private void writeback() {
        while (!_shutdown.get()) {
            try {
                QueueEntry got
                        = _watermarkReached.get()
                        ? _writeQueue.getNoDelay()
                        : _writeQueue.get();

                try {
                    _currentSize.addAndGet(-got._size);
                    flushOne(got._obj);
                } catch (Exception e) {
                    Log.error("Failed writing object " + got._obj.getName() + ", will retry.", e);
                    try {
                        got._obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                            var size = jObjectSizeEstimator.estimateObjectSize(d);
                            _currentSize.addAndGet(size);
                            _writeQueue.add(new QueueEntry(got._obj, size));
                            return null;
                        });
                    } catch (DeletedObjectAccessException ignored) {
                    }
                }
            } catch (InterruptedException ignored) {
            }
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
        m.markWritten();
        if (m.isDeleted()) {
            if (!m.isDeletionCandidate())
                throw new IllegalStateException("Object deleted but not deletable! " + m.getName());
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

    public void remove(JObject<?> object) {
        object.assertRWLock();
        var got = _writeQueue.remove(new QueueEntry(object, 0));
        if (got == null) return;
        _currentSize.addAndGet(-got._size);
    }

    public void markDirty(JObject<?> object) {
        object.assertRWLock();
        if (object.isDeleted() && !object.getMeta().isWritten()) {
            remove(object);
            return;
        }

        if (_currentSize.get() > (watermarkHighRatio * sizeLimit)) {
            if (!_watermarkReached.get()) {
                Log.trace("Watermark reached");
                _watermarkReached.set(true);
                _writeQueue.interrupt();
            }
        } else if (_currentSize.get() <= (watermarkLowRatio * sizeLimit)) {
            if (_watermarkReached.get())
                Log.trace("Watermark reset");
            _watermarkReached.set(false);
        }

        if (_currentSize.get() > sizeLimit) {
            try {
                flushOneImmediate(object.getMeta(), object.getData());
                return;
            } catch (Exception e) {
                Log.error("Failed writing object " + object.getName(), e);
                throw e;
            }
        }

        var size = jObjectSizeEstimator.estimateObjectSize(object.getData());

        var old = _writeQueue.readd(new QueueEntry(object, size));

        if (old != null)
            _currentSize.addAndGet(size - old._size);
        else
            _currentSize.addAndGet(size);
    }
}
