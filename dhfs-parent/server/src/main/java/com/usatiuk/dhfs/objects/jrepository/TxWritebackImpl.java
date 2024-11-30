package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.utils.VoidFn;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.Getter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class TxWritebackImpl implements TxWriteback {
    private final LinkedList<TxBundle> _pendingBundles = new LinkedList<>();
    private final LinkedHashMap<Long, TxBundle> _notFlushedBundles = new LinkedHashMap<>();

    private final Object _flushWaitSynchronizer = new Object();
    private final AtomicLong _lastWrittenTx = new AtomicLong(-1);
    private final AtomicLong _counter = new AtomicLong();
    private final AtomicLong _waitedTotal = new AtomicLong(0);
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    private long currentSize = 0;
    private ExecutorService _writebackExecutor;
    private ExecutorService _commitExecutor;
    private ExecutorService _statusExecutor;
    private volatile boolean _ready = false;

    void init(@Observes @Priority(110) StartupEvent event) {
        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("tx-writeback-%d")
                    .build();

            _writebackExecutor = Executors.newSingleThreadExecutor(factory);
            _writebackExecutor.submit(this::writeback);
        }

        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("writeback-commit-%d")
                    .build();

            _commitExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), factory);
        }
        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    if (currentSize > 0)
                        Log.info("Tx commit status: size="
                                + currentSize / 1024 / 1024 + "MB");
                }
            } catch (InterruptedException ignored) {
            }
        });
        _ready = true;
    }

    void shutdown(@Observes @Priority(890) ShutdownEvent event) throws InterruptedException {
        Log.info("Waiting for all transactions to drain");

        synchronized (_flushWaitSynchronizer) {
            _ready = false;
            while (currentSize > 0) {
                _flushWaitSynchronizer.wait();
            }
        }

        _writebackExecutor.shutdownNow();
        Log.info("Total tx bundle wait time: " + _waitedTotal.get() + "ms");
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Not doing transactions while shutting down!");
    }

    private void writeback() {
        while (!Thread.interrupted()) {
            try {
                TxBundle bundle = new TxBundle(0);
                synchronized (_pendingBundles) {
                    while (_pendingBundles.isEmpty() || !_pendingBundles.peek()._ready)
                        _pendingBundles.wait();

                    long diff = 0;
                    while (!_pendingBundles.isEmpty() && _pendingBundles.peek()._ready) {
                        var toCompress = _pendingBundles.poll();
                        diff -= toCompress.calculateTotalSize();
                        bundle.compress(toCompress);
                    }
                    diff += bundle.calculateTotalSize();
                    synchronized (_flushWaitSynchronizer) {
                        currentSize += diff;
                    }
                }

                var latch = new CountDownLatch(bundle._committed.size() + bundle._meta.size());
                ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

                for (var c : bundle._committed.values()) {
                    _commitExecutor.execute(() -> {
                        try {
                            Log.trace("Writing new " + c.newMeta.getName());
                            objectPersistentStore.writeNewObject(c.newMeta.getName(), c.newMeta, c.newData);
                        } catch (Throwable t) {
                            Log.error("Error writing " + c.newMeta.getName(), t);
                            errors.add(t);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                for (var c : bundle._meta.values()) {
                    _commitExecutor.execute(() -> {
                        try {
                            Log.trace("Writing (meta) " + c.newMeta.getName());
                            objectPersistentStore.writeNewObjectMeta(c.newMeta.getName(), c.newMeta);
                        } catch (Throwable t) {
                            Log.error("Error writing " + c.newMeta.getName(), t);
                            errors.add(t);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                if (Log.isDebugEnabled())
                    for (var d : bundle._deleted.keySet())
                        Log.debug("Deleting from persistent storage " + d.getMeta().getName()); // FIXME: For tests

                latch.await();
                if (!errors.isEmpty()) {
                    throw new RuntimeException("Errors in writeback!");
                }
                objectPersistentStore.commitTx(
                        new TxManifest(
                                Stream.concat(bundle._committed.keySet().stream().map(t -> t.getMeta().getName()),
                                        bundle._meta.keySet().stream().map(t -> t.getMeta().getName())).collect(Collectors.toCollection(ArrayList::new)),
                                bundle._deleted.keySet().stream().map(t -> t.getMeta().getName()).collect(Collectors.toCollection(ArrayList::new))
                        ));
                Log.trace("Bundle " + bundle.getId() + " committed");


                List<List<VoidFn>> callbacks = new ArrayList<>();
                synchronized (_notFlushedBundles) {
                    _lastWrittenTx.set(bundle.getId());
                    while (!_notFlushedBundles.isEmpty() && _notFlushedBundles.firstEntry().getKey() <= bundle.getId()) {
                        callbacks.add(_notFlushedBundles.pollFirstEntry().getValue().setCommitted());
                    }
                }
                callbacks.forEach(l -> l.forEach(VoidFn::apply));

                synchronized (_flushWaitSynchronizer) {
                    currentSize -= ((TxBundle) bundle).calculateTotalSize();
                    // FIXME:
                    if (currentSize <= sizeLimit || !_ready)
                        _flushWaitSynchronizer.notifyAll();
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

    @Override
    public com.usatiuk.dhfs.objects.jrepository.TxBundle createBundle() {
        verifyReady();
        boolean wait = false;
        while (true) {
            if (wait) {
                synchronized (_flushWaitSynchronizer) {
                    long started = System.currentTimeMillis();
                    while (currentSize > sizeLimit) {
                        try {
                            _flushWaitSynchronizer.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    long waited = System.currentTimeMillis() - started;
                    _waitedTotal.addAndGet(waited);
                    if (Log.isTraceEnabled())
                        Log.trace("Thread " + Thread.currentThread().getName() + " waited for tx bundle for " + waited + " ms");
                    wait = false;
                }
            }
            synchronized (_pendingBundles) {
                synchronized (_flushWaitSynchronizer) {
                    if (currentSize > sizeLimit) {
                        if (!_pendingBundles.isEmpty() && _pendingBundles.peek()._ready) {
                            var target = _pendingBundles.poll();

                            long diff = -target.calculateTotalSize();
                            while (!_pendingBundles.isEmpty() && _pendingBundles.peek()._ready) {
                                var toCompress = _pendingBundles.poll();
                                diff -= toCompress.calculateTotalSize();
                                target.compress(toCompress);
                            }
                            diff += target.calculateTotalSize();
                            currentSize += diff;
                            _pendingBundles.addFirst(target);
                        }
                    }

                    if (currentSize > sizeLimit) {
                        wait = true;
                        continue;
                    }
                }
                synchronized (_notFlushedBundles) {
                    var bundle = new TxBundle(_counter.incrementAndGet());
                    _pendingBundles.addLast(bundle);
                    _notFlushedBundles.put(bundle.getId(), bundle);
                    return bundle;
                }
            }
        }
    }

    @Override
    public void commitBundle(com.usatiuk.dhfs.objects.jrepository.TxBundle bundle) {
        verifyReady();
        synchronized (_pendingBundles) {
            ((TxBundle) bundle).setReady();
            if (_pendingBundles.peek() == bundle)
                _pendingBundles.notify();
            synchronized (_flushWaitSynchronizer) {
                currentSize += ((TxBundle) bundle).calculateTotalSize();
            }
        }
    }

    @Override
    public void dropBundle(com.usatiuk.dhfs.objects.jrepository.TxBundle bundle) {
        verifyReady();
        synchronized (_pendingBundles) {
            Log.warn("Dropped bundle: " + bundle);
            _pendingBundles.remove((TxBundle) bundle);
            synchronized (_flushWaitSynchronizer) {
                currentSize -= ((TxBundle) bundle).calculateTotalSize();
            }
        }
    }

    @Override
    public void fence(long bundleId) {
        var latch = new CountDownLatch(1);
        asyncFence(bundleId, latch::countDown);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void asyncFence(long bundleId, VoidFn fn) {
        verifyReady();
        if (bundleId < 0) throw new IllegalArgumentException("txId should be >0!");
        if (_lastWrittenTx.get() >= bundleId) {
            fn.apply();
            return;
        }
        synchronized (_notFlushedBundles) {
            if (_lastWrittenTx.get() >= bundleId) {
                fn.apply();
                return;
            }
            _notFlushedBundles.get(bundleId).addCallback(fn);
        }
    }

    @Getter
    private static class TxManifest implements com.usatiuk.dhfs.objects.repository.persistence.TxManifest {
        private final ArrayList<String> _written;
        private final ArrayList<String> _deleted;

        private TxManifest(ArrayList<String> written, ArrayList<String> deleted) {
            _written = written;
            _deleted = deleted;
        }
    }

    private class TxBundle implements com.usatiuk.dhfs.objects.jrepository.TxBundle {
        private final HashMap<JObject<?>, CommittedEntry> _committed = new HashMap<>();
        private final HashMap<JObject<?>, CommittedMeta> _meta = new HashMap<>();
        private final HashMap<JObject<?>, Integer> _deleted = new HashMap<>();
        private final ArrayList<VoidFn> _callbacks = new ArrayList<>();
        private long _txId;
        @Getter
        private volatile boolean _ready = false;
        private long _size = -1;
        private boolean _wasCommitted = false;

        private TxBundle(long txId) {_txId = txId;}

        @Override
        public long getId() {
            return _txId;
        }

        public void setReady() {
            _ready = true;
        }

        public void addCallback(VoidFn callback) {
            synchronized (_callbacks) {
                if (_wasCommitted) throw new IllegalStateException();
                _callbacks.add(callback);
            }
        }

        public List<VoidFn> setCommitted() {
            synchronized (_callbacks) {
                _wasCommitted = true;
                return Collections.unmodifiableList(_callbacks);
            }
        }

        @Override
        public void commit(JObject<?> obj, ObjectMetadataP meta, JObjectDataP data) {
            synchronized (_committed) {
                _committed.put(obj, new CommittedEntry(meta, data, obj.estimateSize()));
            }
        }

        @Override
        public void commitMetaChange(JObject<?> obj, ObjectMetadataP meta) {
            synchronized (_meta) {
                _meta.put(obj, new CommittedMeta(meta, obj.estimateSize()));
            }
        }

        @Override
        public void delete(JObject<?> obj) {
            synchronized (_deleted) {
                _deleted.put(obj, obj.estimateSize());
            }
        }


        public long calculateTotalSize() {
            if (_size >= 0) return _size;
            long out = 0;
            for (var c : _committed.values())
                out += c.size;
            for (var c : _meta.values())
                out += c.size;
            for (var c : _deleted.entrySet())
                out += c.getValue();
            _size = out;
            return _size;
        }

        public void compress(TxBundle other) {
            if (_txId >= other._txId)
                throw new IllegalArgumentException("Compressing an older bundle into newer");

            _txId = other._txId;
            _size = -1;

            for (var d : other._deleted.entrySet()) {
                _committed.remove(d.getKey());
                _meta.remove(d.getKey());
                _deleted.put(d.getKey(), d.getValue());
            }

            for (var c : other._committed.entrySet()) {
                _committed.put(c.getKey(), c.getValue());
                _meta.remove(c.getKey());
                _deleted.remove(c.getKey());
            }

            for (var m : other._meta.entrySet()) {
                var deleted = _deleted.remove(m.getKey());
                if (deleted != null) {
                    _committed.put(m.getKey(), new CommittedEntry(m.getValue().newMeta, null, m.getKey().estimateSize()));
                    continue;
                }
                var committed = _committed.remove(m.getKey());
                if (committed != null) {
                    _committed.put(m.getKey(), new CommittedEntry(m.getValue().newMeta, committed.newData, m.getKey().estimateSize()));
                    continue;
                }
                _meta.put(m.getKey(), m.getValue());
            }
        }

        private record CommittedEntry(ObjectMetadataP newMeta, JObjectDataP newData, int size) {}

        private record CommittedMeta(ObjectMetadataP newMeta, int size) {}

        private record Deleted(JObject<?> handle) {}
    }
}
