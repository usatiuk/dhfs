package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperImpl;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.objects.transaction.TxCommitException;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

@ApplicationScoped
public class WritebackObjectPersistentStore {
    @Inject
    CachingObjectPersistentStore cachedStore;

    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;

    private final AtomicReference<TxBundle> _pendingBundle = new AtomicReference<>(null);
    private final AtomicReference<PendingWriteData> _pendingWrites = new AtomicReference<>(null);

    private final Object _flushWaitSynchronizer = new Object();

    private final AtomicLong _lastFlushedId = new AtomicLong(-1);
    private final AtomicLong _lastCommittedId = new AtomicLong(-1);

    private final AtomicLong _waitedTotal = new AtomicLong(0);
    private long currentSize = 0;

    private ExecutorService _writebackExecutor;
    private ExecutorService _statusExecutor;

    @Inject
    ExecutorService _callbackExecutor;

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
                    if (currentSize > 0)
                        Log.info("Tx commit status: size=" + currentSize / 1024 / 1024 + "MB");
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
                TxBundle bundle;
                synchronized (_pendingBundle) {
                    while ((bundle = _pendingBundle.getAndSet(null)) == null)
                        _pendingBundle.wait();
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

                synchronized (_pendingWrites) {
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
                }

                _lastFlushedId.set(bundle.id());
                var callbacks = bundle.getCallbacks();
                _callbackExecutor.submit(() -> {
                    callbacks.forEach(Runnable::run);
                });

                synchronized (_flushWaitSynchronizer) {
                    currentSize -= bundle.size();
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

    public long commitBundle(Collection<TxRecord.TxObjectRecord<?>> writes) {
        verifyReady();
        while (true) {
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
                    Log.tracev("Thread {0} waited for tx bundle for {1} ms", Thread.currentThread().getName(), waited);
            }

            synchronized (_pendingBundle) {
                synchronized (_flushWaitSynchronizer) {
                    if (currentSize > sizeLimit) {
                        continue;
                    }
                }

                TxBundle bundle = new TxBundle(_lastCommittedId.incrementAndGet());

                for (var action : writes) {
                    switch (action) {
                        case TxRecord.TxObjectRecordWrite<?> write -> {
//                            Log.tracev("Flushing object {0}", write.key());
                            bundle.commit(new JDataVersionedWrapperImpl(write.data(), bundle.id()));
                        }
                        case TxRecord.TxObjectRecordDeleted deleted -> {
//                            Log.tracev("Deleting object {0}", deleted.key());
                            bundle.delete(deleted.key());
                        }
                        default -> {
                            throw new TxCommitException("Unexpected value: " + action.key());
                        }
                    }
                }

                synchronized (_pendingWrites) {
                    var curPw = _pendingWrites.get();
                    var curPwMap = curPw.pendingWrites();
                    for (var e : ((TxBundle) bundle)._entries.values()) {
                        switch (e) {
                            case TxBundle.CommittedEntry c -> {
                                curPwMap = curPwMap.plus(c.key(), new PendingWrite(c.data, bundle.id()));
                            }
                            case TxBundle.DeletedEntry d -> {
                                curPwMap = curPwMap.plus(d.key(), new PendingDelete(d.key, bundle.id()));
                            }
                            default -> throw new IllegalStateException("Unexpected value: " + e);
                        }
                    }
                    // Now, make the changes visible to new iterators
                    var newCurPw = new PendingWriteData(
                            curPwMap,
                            curPw.lastFlushedId(),
                            bundle.id()
                    );

                    _pendingWrites.compareAndSet(curPw, newCurPw);
                }

                var curBundle = _pendingBundle.get();
                long oldSize = 0;
                if (curBundle != null) {
                    oldSize = curBundle.size();
                    curBundle.compress(bundle);
                } else {
                    curBundle = bundle;
                }
                _pendingBundle.set(curBundle);
                _pendingBundle.notifyAll();
                synchronized (_flushWaitSynchronizer) {
                    currentSize += (curBundle.size() - oldSize);
                }

                return bundle.id();
            }
        }
    }

    public void asyncFence(long bundleId, Runnable fn) {
        verifyReady();
        if (bundleId < 0) throw new IllegalArgumentException("txId should be >0!");
        if (_lastFlushedId.get() >= bundleId) {
            fn.run();
            return;
        }
        synchronized (_pendingBundle) {
            if (_lastFlushedId.get() >= bundleId) {
                fn.run();
                return;
            }
            var pendingBundle = _pendingBundle.get();
            if (pendingBundle == null) {
                fn.run();
                return;
            }
            pendingBundle.addCallback(fn);
        }
    }

    public Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes) {
        long bundleId = commitBundle(writes);

        return r -> asyncFence(bundleId, r);
    }

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
        private long _txId;
        private long _size = 0;

        ArrayList<Runnable> getCallbacks() {
            return _callbacks;
        }

        private TxBundle(long txId) {
            _txId = txId;
        }

        public long id() {
            return _txId;
        }

        public void addCallback(Runnable callback) {
            _callbacks.add(callback);
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

        public long size() {
            return _size;
        }

        public void compress(TxBundle other) {
            if (_txId >= other._txId)
                throw new IllegalArgumentException("Compressing an older bundle into newer");

            _txId = other._txId;

            for (var entry : other._entries.values()) {
                putEntry(entry);
            }

            _callbacks.addAll(other._callbacks);
        }

        private interface BundleEntry {
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
