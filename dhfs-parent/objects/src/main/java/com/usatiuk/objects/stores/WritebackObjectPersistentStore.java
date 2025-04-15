package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperImpl;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.objects.transaction.TxCommitException;
import com.usatiuk.objects.transaction.TxRecord;
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

@ApplicationScoped
public class WritebackObjectPersistentStore {
    private final LinkedList<TxBundle> _pendingBundles = new LinkedList<>();
    private final LinkedHashMap<Long, TxBundle> _notFlushedBundles = new LinkedHashMap<>();

    private record PendingWriteData(TreePMap<JObjectKey, PendingWriteEntry> pendingWrites,
                                    long lastFlushedId,
                                    long lastCommittedId) {
    }

    private final AtomicReference<PendingWriteData> _pendingWrites = new AtomicReference<>(null);

    private final Object _flushWaitSynchronizer = new Object();

    private final AtomicLong _lastWrittenId = new AtomicLong(-1);
    private final AtomicLong _lastCommittedId = new AtomicLong();

    private final AtomicLong _waitedTotal = new AtomicLong(0);
    @Inject
    CachingObjectPersistentStore cachedStore;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    private long currentSize = 0;
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
                TxBundle bundle = new TxBundle(0);
                synchronized (_pendingBundles) {
                    while (_pendingBundles.isEmpty() || !_pendingBundles.peek()._ready)
                        _pendingBundles.wait();

                    long diff = 0;
                    while (!_pendingBundles.isEmpty() && _pendingBundles.peek()._ready) {
                        var toCompress = _pendingBundles.poll();
                        diff -= toCompress.size();
                        bundle.compress(toCompress);
                    }
                    diff += bundle.size();
                    synchronized (_flushWaitSynchronizer) {
                        currentSize += diff;
                    }
                }

                var toWrite = new ArrayList<Pair<JObjectKey, JDataVersionedWrapper>>();
                var toDelete = new ArrayList<JObjectKey>();

                for (var e : bundle._entries.values()) {
                    switch (e) {
                        case TxBundle.CommittedEntry(JObjectKey key, JDataVersionedWrapper data, int size) -> {
                            Log.trace("Writing new " + key);
                            toWrite.add(Pair.of(key, data));
                        }
                        case TxBundle.DeletedEntry(JObjectKey key) -> {
                            Log.trace("Deleting from persistent storage " + key);
                            toDelete.add(key);
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + e);
                    }
                }

                cachedStore.commitTx(
                        new TxManifestObj<>(
                                Collections.unmodifiableList(toWrite),
                                Collections.unmodifiableList(toDelete)
                        ), bundle.id());

                Log.trace("Bundle " + bundle.id() + " committed");

                while (true) {
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
                    if (_pendingWrites.compareAndSet(curPw, newCurPw))
                        break;
                }

                List<List<Runnable>> callbacks = new ArrayList<>();
                synchronized (_notFlushedBundles) {
                    _lastWrittenId.set(bundle.id());
                    while (!_notFlushedBundles.isEmpty() && _notFlushedBundles.firstEntry().getKey() <= bundle.id()) {
                        callbacks.add(_notFlushedBundles.pollFirstEntry().getValue().setCommitted());
                    }
                }
                callbacks.forEach(l -> l.forEach(Runnable::run));

                synchronized (_flushWaitSynchronizer) {
                    currentSize -= bundle.size();
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

    public long commitBundle(Collection<TxRecord.TxObjectRecord<?>> writes) {
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

                            long diff = -target.size();
                            while (!_pendingBundles.isEmpty() && _pendingBundles.peek()._ready) {
                                var toCompress = _pendingBundles.poll();
                                diff -= toCompress.size();
                                target.compress(toCompress);
                            }
                            diff += target.size();
                            currentSize += diff;
                            _pendingBundles.addFirst(target);
                        }
                    }

                    if (currentSize > sizeLimit) {
                        wait = true;
                        continue;
                    }
                }

                TxBundle bundle;
                synchronized (_notFlushedBundles) {
                    bundle = new TxBundle(_lastCommittedId.incrementAndGet());
                    _pendingBundles.addLast(bundle);
                    _notFlushedBundles.put(bundle.id(), bundle);
                }

                for (var action : writes) {
                    switch (action) {
                        case TxRecord.TxObjectRecordWrite<?> write -> {
                            Log.trace("Flushing object " + write.key());
                            bundle.commit(new JDataVersionedWrapperImpl(write.data(), bundle.id()));
                        }
                        case TxRecord.TxObjectRecordDeleted deleted -> {
                            Log.trace("Deleting object " + deleted.key());
                            bundle.delete(deleted.key());
                        }
                        default -> {
                            throw new TxCommitException("Unexpected value: " + action.key());
                        }
                    }
                }

                while (true) {
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

                    if (!_pendingWrites.compareAndSet(curPw, newCurPw))
                        continue;

                    ((TxBundle) bundle).setReady();
                    if (_pendingBundles.peek() == bundle)
                        _pendingBundles.notify();
                    synchronized (_flushWaitSynchronizer) {
                        currentSize += ((TxBundle) bundle).size();
                    }

                    return bundle.id();
                }
            }
        }
    }

    public void asyncFence(long bundleId, Runnable fn) {
        verifyReady();
        if (bundleId < 0) throw new IllegalArgumentException("txId should be >0!");
        if (_lastWrittenId.get() >= bundleId) {
            fn.run();
            return;
        }
        synchronized (_notFlushedBundles) {
            if (_lastWrittenId.get() >= bundleId) {
                fn.run();
                return;
            }
            _notFlushedBundles.get(bundleId).addCallback(fn);
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
                public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
                    return new TombstoneMergingKvIterator<>("writeback-ps", start, key,
                            (tS, tK) -> new MappingKvIterator<>(
                                    new NavigableMapKvIterator<>(_pendingWrites, tS, tK),
                                    e -> switch (e) {
                                        case PendingWrite pw -> new Data<>(pw.data());
                                        case PendingDelete d -> new Tombstone<>();
                                        default -> throw new IllegalStateException("Unexpected value: " + e);
                                    }),
                            (tS, tK) -> new MappingKvIterator<>(_cache.getIterator(tS, tK), Data::new));
                }

                @Nonnull
                @Override
                public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                    var cached = _pendingWrites.get(name);
                    if (cached != null) {
                        return switch (cached) {
                            case PendingWrite c -> Optional.of(c.data());
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

    public interface VerboseReadResult {
    }

    private static class TxBundle {
        private final LinkedHashMap<JObjectKey, BundleEntry> _entries = new LinkedHashMap<>();
        private final ArrayList<Runnable> _callbacks = new ArrayList<>();
        private long _txId;
        private volatile boolean _ready = false;
        private long _size = 0;
        private boolean _wasCommitted = false;

        private TxBundle(long txId) {
            _txId = txId;
        }

        public long id() {
            return _txId;
        }

        public void setReady() {
            _ready = true;
        }

        public void addCallback(Runnable callback) {
            synchronized (_callbacks) {
                if (_wasCommitted) throw new IllegalStateException();
                _callbacks.add(callback);
            }
        }

        public List<Runnable> setCommitted() {
            synchronized (_callbacks) {
                _wasCommitted = true;
                return Collections.unmodifiableList(_callbacks);
            }
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

            synchronized (_callbacks) {
                assert !_wasCommitted;
                assert !other._wasCommitted;
                _callbacks.addAll(other._callbacks);
            }
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

    public record VerboseReadResultPersisted(Optional<JDataVersionedWrapper> data) implements VerboseReadResult {
    }

    public record VerboseReadResultPending(PendingWriteEntry pending) implements VerboseReadResult {
    }
}
