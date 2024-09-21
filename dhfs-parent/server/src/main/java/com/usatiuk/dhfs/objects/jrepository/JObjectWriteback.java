package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class JObjectWriteback {
    private final HashSetDelayedBlockingQueue<QueueEntry> _writeQueue;
    private final AtomicLong _currentSize = new AtomicLong(0);
    private final AtomicBoolean _watermarkReached = new AtomicBoolean(false);
    private final AtomicBoolean _shutdown = new AtomicBoolean(false);
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
    @Inject
    ProtoSerializer<ObjectMetadataP, ObjectMetadata> metaProtoSerializer;
    @Inject
    JObjectTxManager jObjectTxManager;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    @ConfigProperty(name = "dhfs.objects.writeback.watermark-high")
    float watermarkHighRatio;
    @ConfigProperty(name = "dhfs.objects.writeback.watermark-low")
    float watermarkLowRatio;
    @ConfigProperty(name = "dhfs.objects.writeback.threads")
    int writebackThreads;
    @ConfigProperty(name = "dhfs.objects.writeback.delay")
    long promotionDelay;
    private ExecutorService _writebackExecutor;
    private ExecutorService _statusExecutor;
    private AtomicLong _waitedTotal = new AtomicLong(0);

    public JObjectWriteback(@ConfigProperty(name = "dhfs.objects.writeback.delay") long promotionDelay) {
        _writeQueue = new HashSetDelayedBlockingQueue<>(promotionDelay);
    }

    void init(@Observes @Priority(110) StartupEvent event) {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("writeback-%d")
                .build();

        _writebackExecutor = Executors.newFixedThreadPool(writebackThreads, factory);
        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    if (_currentSize.get() > 0)
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

    void shutdown(@Observes @Priority(890) ShutdownEvent event) {
        _shutdown.set(true);
        _writebackExecutor.shutdownNow();
        _statusExecutor.shutdownNow();

        var toWrite = _writeQueue.close();

        Log.info("Flushing objects");
        for (var v : toWrite) {
            try {
                flushOne(v._obj, v._dataDirty);
            } catch (Exception e) {
                Log.error("Failed writing object " + v._obj.getMeta().getName(), e);
            }
        }
        Log.info("Total writeback wait time: " + _waitedTotal.get() + "ms");
    }

    private void writeback() {
        while (!_shutdown.get()) {
            try {
                QueueEntry got = _writeQueue.get();

                try {
                    _currentSize.addAndGet(-got._size);
                    flushOne(got._obj, got._dataDirty);
                    if (_currentSize.get() <= sizeLimit)
                        synchronized (this) {
                            this.notifyAll();
                        }
                } catch (Exception e) {
                    try {
                        got._obj.runReadLockedVoid(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                            doAdd(got._obj, got._dataDirty);
                        });
                        Log.error("Failed writing object " + got._obj.getMeta().getName() + ", will retry.", e);
                    } catch (DeletedObjectAccessException ignored) {
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                Log.error("Uncaught exception in writeback", e);
            } catch (Throwable o) {
                Log.error("Uncaught THROWABLE in writeback", o);
            }
        }
        Log.info("Writeback thread exiting");
    }

    private void flushOne(JObject<?> obj, boolean dataChanged) {
        if (obj.getMeta().isDeleted()) {
            // FIXME
            jObjectTxManager.executeTx(() -> {
                obj.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, data, b, i) -> {
                    flushOneImmediate(m, data, dataChanged);
                    return null;
                });
            });
        } else
            obj.runReadLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, data) -> {
                flushOneImmediate(m, data, dataChanged);
                return null;
            });
    }

    private <T extends JObjectData> void flushOneImmediate(ObjectMetadata m, T data, boolean dataChanged) {
        Log.trace("Flushing " + m.getName());
        m.markWritten();
        if (m.isDeleted()) {
            if (!m.isDeletionCandidate())
                throw new IllegalStateException("Object deleted but not deletable! " + m.getName());
            // FIXME: assert Rw lock here?
            Log.debug("Deleting from persistent storage " + m.getName());
            objectPersistentStore.deleteObjectDirect(m.getName());
            m.markUnWritten();
            return;
        }
        // Can be unnecessary
        if (m.isHaveLocalCopy() && (data != null && dataChanged))
            objectPersistentStore.writeObjectDirect(m.getName(), metaProtoSerializer.serialize(m), dataProtoSerializer.serialize(data));
        else if (m.isHaveLocalCopy() && !dataChanged)
            objectPersistentStore.writeObjectMetaDirect(m.getName(), metaProtoSerializer.serialize(m));
        else if (!m.isHaveLocalCopy())
            objectPersistentStore.writeObjectDirect(m.getName(), metaProtoSerializer.serialize(m), null);
        else
            throw new IllegalStateException("Unexpected object flush combination");
    }

    public void remove(JObject<?> object) {
        var got = _writeQueue.remove(new QueueEntry(object, 0, false));
        if (got == null) return;
        _currentSize.addAndGet(-got._size);
    }

    // Object should be at least read-locked
    public void markDirty(JObject<?> object, boolean dataChanged) {
        if (object.getMeta().isDeleted() && !object.getMeta().isWritten()) {
            remove(object);
            return;
        }

        if (_currentSize.get() > (watermarkHighRatio * sizeLimit)) {
            if (!_watermarkReached.get()) {
                Log.trace("Watermark reached");
                _watermarkReached.set(true);
                _writeQueue.setDelay(0);
            }
        } else if (_currentSize.get() <= (watermarkLowRatio * sizeLimit)) {
            if (_watermarkReached.get()) {
                Log.trace("Watermark reset");
                _watermarkReached.set(false);
                _writeQueue.setDelay(promotionDelay);
            }
        }

        if (!object.getMeta().isDeleted() && _currentSize.get() > sizeLimit) {
            long started = System.currentTimeMillis();
            final long timeout = 100L; // FIXME:
            boolean finished = false;
            while (!finished && System.currentTimeMillis() - started < timeout) {
                synchronized (this) {
                    try {
                        this.wait(timeout);
                        finished = true;
                    } catch (InterruptedException ignored) {
                    }
                }
                if (_currentSize.get() > sizeLimit)
                    finished = false;
            }
            if (!finished) {
                Log.error("Timed out waiting for writeback to drain");
                try {
                    flushOneImmediate(object.getMeta(), object.getData(), dataChanged);
                    return;
                } catch (Exception e) {
                    Log.error("Failed writing object " + object.getMeta().getName(), e);
                    throw e;
                }
            } else {
                long waited = System.currentTimeMillis() - started;
                _waitedTotal.addAndGet(waited);
                if (Log.isTraceEnabled())
                    Log.trace("Thread " + Thread.currentThread().getName() + " waited for writeback for " + waited + " ms");
            }
        }

        doAdd(object, dataChanged);
    }

    private void doAdd(JObject<?> object, boolean dirtyData) {
        var size = object.estimateSize();
        // FIXME: queueEntry twice
        var old = _writeQueue.merge(new QueueEntry(object, 0, false), (prev) -> {
            boolean newDataDirty = false;
            if (prev != null)
                newDataDirty |= prev._dataDirty;
            newDataDirty |= dirtyData;
            return new QueueEntry(object, size, newDataDirty);
        });

        if (old != null)
            _currentSize.addAndGet(size - old._size);
        else
            _currentSize.addAndGet(size);
    }

    private class QueueEntry {
        private final JObject<?> _obj;
        private final long _size;
        private final boolean _dataDirty;

        private QueueEntry(JObject<?> obj, long size, boolean dataDirty) {
            _obj = obj;
            _size = size;
            _dataDirty = dataDirty;
        }

        // FIXME: That's a little bit of a hack
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
}
