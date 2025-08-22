package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperImpl;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;
import com.usatiuk.objects.iterators.NavigableMapKvIterator;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.objects.transaction.TxRecord;
import com.usatiuk.utils.ListUtils;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.PSortedMap;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Asynchronous write cache of objects.
 * Objects are put into a write queue by commitTx, and written to the storage by a separate thread.
 */
@ApplicationScoped
public class WritebackObjectPersistentStore {
    @Inject
    CachingObjectPersistentStore cachedStore;
    @Inject
    ExecutorService _callbackExecutor;

    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    int sizeLimit;

    private TxBundle _pendingBundle = null;
    private int _curSize = 0;

    private final AtomicReference<PendingWriteData> _pendingWrites = new AtomicReference<>(null);

    private final ReentrantLock _pendingBundleLock = new ReentrantLock();

    private final Condition _newBundleCondition = _pendingBundleLock.newCondition();
    private final Condition _flushCondition = _pendingBundleLock.newCondition();

    private final AtomicLong _lastFlushedId = new AtomicLong(-1);
    private final AtomicLong _lastCommittedId = new AtomicLong(-1);

    private final AtomicLong _waitedTotal = new AtomicLong(0);

    private ExecutorService _writebackExecutor;
    private ExecutorService _statusExecutor;

    private volatile boolean _ready = false;

    void init(@Observes @Priority(120) StartupEvent event) {
        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("tx-writeback-%d")
                    .build();

            _writebackExecutor = Executors.newSingleThreadExecutor(factory);
            _writebackExecutor.submit(this::writeback);
        }

        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    if (_curSize > 0)
                        Log.info("Tx commit status: size=" + _curSize / 1024 / 1024 + "MB");
                }
            } catch (InterruptedException ignored) {
            }
        });
        long lastTxId;
        try (var s = cachedStore.getSnapshot()) {
            lastTxId = s.id();
        }
        _lastCommittedId.set(lastTxId);
        _lastFlushedId.set(lastTxId);
        _pendingWrites.set(new PendingWriteData(TreePMap.empty(), lastTxId, lastTxId));
        _ready = true;
    }

    void shutdown(@Observes @Priority(890) ShutdownEvent event) throws InterruptedException {
        Log.info("Waiting for all transactions to drain");

        _ready = false;
        _pendingBundleLock.lock();
        try {
            while (_curSize > 0) {
                _flushCondition.await();
            }
        } finally {
            _pendingBundleLock.unlock();
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
                TxBundle bundle;
                _pendingBundleLock.lock();
                try {
                    while (_pendingBundle == null)
                        _newBundleCondition.await();
                    bundle = _pendingBundle;
                    _pendingBundle = null;

                    _curSize -= bundle.size();
                    assert _curSize == 0;
                    _flushCondition.signal();
                } finally {
                    _pendingBundleLock.unlock();
                }

                var toWrite = new ArrayList<Pair<JObjectKey, JDataVersionedWrapper>>();
                var toDelete = new ArrayList<JObjectKey>();

                for (var e : bundle._entries.values()) {
                    switch (e) {
                        case TxBundle.CommittedEntry(JObjectKey key, JDataVersionedWrapper data, int size) -> {
                            Log.tracev("Writing new {0}", key);
                            toWrite.add(Pair.of(key, data));
                        }
                        case TxBundle.DeletedEntry(JObjectKey key) -> {
                            Log.tracev("Deleting from persistent storage {0}", key);
                            toDelete.add(key);
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + e);
                    }
                }

                cachedStore.commitTx(new TxManifestObj<>(toWrite, toDelete), bundle.id());

                Log.tracev("Bundle {0} committed", bundle.id());

                _pendingBundleLock.lock();
                try {
                    var curPw = _pendingWrites.get();
                    var curPwMap = curPw.pendingWrites();
                    for (var e : bundle._entries.values()) {
                        var cur = curPwMap.get(e.key());
                        if (cur.bundleId() <= bundle.id())
                            curPwMap = curPwMap.minus(e.key());
                    }
                    var newCurPw = new PendingWriteData(
                            curPwMap,
                            bundle.id(),
                            curPw.lastCommittedId()
                    );
                    _pendingWrites.compareAndSet(curPw, newCurPw);
                } finally {
                    _pendingBundleLock.unlock();
                }

                _lastFlushedId.set(bundle.id());
                var callbacks = bundle.callbacks();
                _callbackExecutor.submit(() -> {
                    callbacks.forEach(Runnable::run);
                });
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                Log.error("Uncaught exception in writeback", e);
            } catch (Throwable o) {
                Log.error("Uncaught THROWABLE in writeback", o);
            }
        }
        Log.info("Writeback thread exiting");
    }

    private long commitBundle(Collection<TxRecord.TxObjectRecord<?>> writes) {
        verifyReady();
        _pendingBundleLock.lock();
        try {
            boolean shouldWake = false;
            if (_curSize > sizeLimit) {
                shouldWake = true;
                long started = System.currentTimeMillis();
                while (_curSize > sizeLimit)
                    _flushCondition.await();
                long waited = System.currentTimeMillis() - started;
                _waitedTotal.addAndGet(waited);
                if (Log.isTraceEnabled())
                    Log.tracev("Thread {0} waited for tx bundle for {1} ms", Thread.currentThread().getName(), waited);
            }

            var oursId = _lastCommittedId.incrementAndGet();

            var curBundle = _pendingBundle;
            int oldSize = 0;
            if (curBundle != null) {
                oldSize = curBundle.size();
                curBundle.setId(oursId);
            } else {
                curBundle = new TxBundle(oursId);
            }

            var curPw = _pendingWrites.get();
            var curPwMap = curPw.pendingWrites();

            for (var action : writes) {
                var key = action.key();
                switch (action) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
//                            Log.tracev("Flushing object {0}", write.key());
                        var wrapper = new JDataVersionedWrapperImpl(write.data(), oursId);
                        curPwMap = curPwMap.plus(key, new PendingWrite(wrapper, oursId));
                        curBundle.commit(wrapper);
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
//                            Log.tracev("Deleting object {0}", deleted.key());
                        curPwMap = curPwMap.plus(key, new PendingDelete(key, oursId));
                        curBundle.delete(key);
                    }
                }
            }
            // Now, make the changes visible to new iterators
            var newCurPw = new PendingWriteData(
                    curPwMap,
                    curPw.lastFlushedId(),
                    oursId
            );

            _pendingWrites.compareAndSet(curPw, newCurPw);

            _pendingBundle = curBundle;
            _newBundleCondition.signalAll();

            _curSize += (curBundle.size() - oldSize);

            if (shouldWake && _curSize < sizeLimit) {
                _flushCondition.signal();
            }

            return oursId;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            _pendingBundleLock.unlock();
        }
    }

    /**
     * Run a given callback after the transaction with id txId is committed.
     * If the transaction is already committed, the callback is run immediately.
     *
     * @param txId transaction id to wait for
     * @param fn   callback to run
     */
    public void asyncFence(long txId, Runnable fn) {
        verifyReady();
        if (txId < 0) throw new IllegalArgumentException("txId should be >0!");
        if (_lastFlushedId.get() >= txId) {
            fn.run();
            return;
        }
        _pendingBundleLock.lock();
        try {
            if (_lastFlushedId.get() >= txId) {
                fn.run();
                return;
            }
            var pendingBundle = _pendingBundle;
            if (pendingBundle == null) {
                fn.run();
                return;
            }
            pendingBundle.addCallback(fn);
        } finally {
            _pendingBundleLock.unlock();
        }
    }

    /**
     * Commit a transaction to the persistent store.
     *
     * @param writes the transaction manifest
     * @return a function that allows to add a callback to be run after the transaction is committed
     */
    public Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes) {
        long bundleId = commitBundle(writes);

        return r -> asyncFence(bundleId, r);
    }

    /**
     * Get the last committed transaction ID.
     *
     * @return the last committed transaction ID
     */
    public long getLastCommitId() {
        return _lastCommittedId.get();
    }

    /**
     * Get a snapshot of the persistent store, including the pending writes.
     *
     * @return a snapshot of the store
     */
    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        Snapshot<JObjectKey, JDataVersionedWrapper> cache = null;
        PendingWriteData pw = null;

        try {
            while (true) {
                pw = _pendingWrites.get();
                cache = cachedStore.getSnapshot();

                if (cache.id() >= pw.lastCommittedId())
                    return cache;

                // TODO: Can this really happen?
                if (cache.id() < pw.lastFlushedId()) {
                    assert false;
                    cache.close();
                    cache = null;
                    continue;
                }

                break;
            }

            PendingWriteData finalPw = pw;
            Snapshot<JObjectKey, JDataVersionedWrapper> finalCache = cache;
            return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
                private final PSortedMap<JObjectKey, PendingWriteEntry> _pendingWrites = finalPw.pendingWrites();
                private final Snapshot<JObjectKey, JDataVersionedWrapper> _cache = finalCache;
                private final long txId = finalPw.lastCommittedId();

                @Override
                public List<CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>> getIterator(IteratorStart start, JObjectKey key) {
                    return ListUtils.prepend(new NavigableMapKvIterator<>(_pendingWrites, start, key), _cache.getIterator(start, key));
                }

                @Nonnull
                @Override
                public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                    var cached = _pendingWrites.get(name);
                    if (cached != null) {
                        return switch (cached) {
                            case PendingWrite c -> Optional.of(c.value());
                            case PendingDelete d -> {
                                yield Optional.empty();
                            }
                            default -> throw new IllegalStateException("Unexpected value: " + cached);
                        };
                    }
                    return _cache.readObject(name);
                }

                @Override
                public long id() {
                    assert txId >= _cache.id();
                    return txId;
                }

                @Override
                public void close() {
                    _cache.close();
                }
            };
        } catch (Throwable e) {
            if (cache != null)
                cache.close();
            throw e;
        }
    }

    private record PendingWriteData(TreePMap<JObjectKey, PendingWriteEntry> pendingWrites,
                                    long lastFlushedId,
                                    long lastCommittedId) {
    }

    private static class TxBundle {
        private final HashMap<JObjectKey, BundleEntry> _entries = new HashMap<>();
        private final ArrayList<Runnable> _callbacks = new ArrayList<>();
        private int _size = 0;
        private long _txId;

        ArrayList<Runnable> callbacks() {
            return _callbacks;
        }

        private TxBundle(long txId) {
            _txId = txId;
        }

        public void setId(long id) {
            _txId = id;
        }

        public long id() {
            return _txId;

        }

        public void addCallback(Runnable callback) {
            _callbacks.add(callback);
        }

        public int size() {
            return _size;
        }

        private void putEntry(BundleEntry entry) {
            var old = _entries.put(entry.key(), entry);
            if (old != null) {
                _size -= old.size();
            }
            _size += entry.size();
        }

        public void commit(JDataVersionedWrapper obj) {
            putEntry(new CommittedEntry(obj.data().key(), obj, obj.data().estimateSize()));
        }

        public void delete(JObjectKey obj) {
            putEntry(new DeletedEntry(obj));
        }

        private sealed interface BundleEntry permits CommittedEntry, DeletedEntry {
            JObjectKey key();

            int size();
        }

        private record CommittedEntry(JObjectKey key, JDataVersionedWrapper data, int size)
                implements BundleEntry {
        }

        private record DeletedEntry(JObjectKey key)
                implements BundleEntry {

            public int size() {
                return 64;
            }
        }
    }
}
