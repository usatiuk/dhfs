package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.objects.persistence.TxManifest;
import com.usatiuk.dhfs.utils.VoidFn;
import com.usatiuk.objects.common.runtime.JObjectKey;
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

@ApplicationScoped
public class TxWritebackImpl implements TxWriteback {
    private final LinkedList<TxBundleImpl> _pendingBundles = new LinkedList<>();
    private final LinkedHashMap<Long, TxBundleImpl> _notFlushedBundles = new LinkedHashMap<>();

    private final Object _flushWaitSynchronizer = new Object();
    private final AtomicLong _lastWrittenTx = new AtomicLong(-1);
    private final AtomicLong _counter = new AtomicLong();
    private final AtomicLong _waitedTotal = new AtomicLong(0);
    @Inject
    ObjectPersistentStore objectPersistentStore;
    @Inject
    ObjectSerializer<JDataVersionedWrapper> objectSerializer;
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
                TxBundleImpl bundle = new TxBundleImpl(0);
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

                var latch = new CountDownLatch((int) bundle._entries.values().stream().filter(e -> e instanceof TxBundleImpl.CommittedEntry).count());
                ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

                for (var e : bundle._entries.values()) {
                    switch (e) {
                        case TxBundleImpl.CommittedEntry c -> _commitExecutor.execute(() -> {
                            try {
                                Log.trace("Writing new " + c.key());
                                objectPersistentStore.writeObject(c.key(), objectSerializer.serialize(c.data()));
                            } catch (Throwable t) {
                                Log.error("Error writing " + c.key(), t);
                                errors.add(t);
                            } finally {
                                latch.countDown();
                            }
                        });
                        case TxBundleImpl.DeletedEntry d -> {
                            if (Log.isDebugEnabled())
                                Log.debug("Deleting from persistent storage " + d.key()); // FIXME: For tests
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + e);
                    }
                }

                latch.await();
                if (!errors.isEmpty()) {
                    throw new RuntimeException("Errors in writeback!");
                }

                objectPersistentStore.commitTx(
                        new TxManifest(
                                bundle._entries.values().stream().filter(e -> e instanceof TxBundleImpl.CommittedEntry).map(TxBundleImpl.BundleEntry::key).toList(),
                                bundle._entries.values().stream().filter(e -> e instanceof TxBundleImpl.DeletedEntry).map(TxBundleImpl.BundleEntry::key).toList()
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
                    currentSize -= ((TxBundleImpl) bundle).calculateTotalSize();
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
    public TxBundle createBundle() {
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
                    var bundle = new TxBundleImpl(_counter.incrementAndGet());
                    _pendingBundles.addLast(bundle);
                    _notFlushedBundles.put(bundle.getId(), bundle);
                    return bundle;
                }
            }
        }
    }

    @Override
    public void commitBundle(TxBundle bundle) {
        verifyReady();
        synchronized (_pendingBundles) {
            ((TxBundleImpl) bundle).setReady();
            if (_pendingBundles.peek() == bundle)
                _pendingBundles.notify();
            synchronized (_flushWaitSynchronizer) {
                currentSize += ((TxBundleImpl) bundle).calculateTotalSize();
            }
        }
    }

    @Override
    public void dropBundle(TxBundle bundle) {
        verifyReady();
        synchronized (_pendingBundles) {
            Log.warn("Dropped bundle: " + bundle);
            _pendingBundles.remove((TxBundleImpl) bundle);
            synchronized (_flushWaitSynchronizer) {
                currentSize -= ((TxBundleImpl) bundle).calculateTotalSize();
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

    private class TxBundleImpl implements TxBundle {
        private final LinkedHashMap<JObjectKey, BundleEntry> _entries = new LinkedHashMap<>();
        private final ArrayList<VoidFn> _callbacks = new ArrayList<>();
        private long _txId;
        @Getter
        private volatile boolean _ready = false;
        private long _size = -1;
        private boolean _wasCommitted = false;

        private TxBundleImpl(long txId) {
            _txId = txId;
        }

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
        public void commit(JDataVersionedWrapper<?> obj) {
            synchronized (_entries) {
                _entries.put(obj.data().key(), new CommittedEntry(obj.data().key(), obj, obj.data().estimateSize()));
            }
        }

        @Override
        public void delete(JObjectKey obj) {
            synchronized (_entries) {
                _entries.put(obj, new DeletedEntry(obj));
            }
        }

        public long calculateTotalSize() {
            if (_size >= 0) return _size;
            _size = _entries.values().stream().mapToInt(BundleEntry::size).sum();
            return _size;
        }

        public void compress(TxBundleImpl other) {
            if (_txId >= other._txId)
                throw new IllegalArgumentException("Compressing an older bundle into newer");

            _txId = other._txId;
            _size = -1;

            _entries.putAll(other._entries);
        }

        private interface BundleEntry {
            JObjectKey key();

            int size();
        }

        private record CommittedEntry(JObjectKey key, JDataVersionedWrapper<?> data, int size)
                implements BundleEntry {
        }

        private record DeletedEntry(JObjectKey key)
                implements BundleEntry {
            @Override
            public int size() {
                return 64;
            }
        }
    }
}
