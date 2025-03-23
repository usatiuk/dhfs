package com.usatiuk.dhfs.objects.stores;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JDataVersionedWrapperImpl;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.*;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;
import com.usatiuk.dhfs.objects.transaction.TxCommitException;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@ApplicationScoped
public class WritebackObjectPersistentStore {
    private final LinkedList<TxBundle> _pendingBundles = new LinkedList<>();

    private final AtomicReference<PSortedMap<JObjectKey, PendingWriteEntry>> _pendingWrites = new AtomicReference<>(TreePMap.empty());
    private final ReentrantReadWriteLock _pendingWritesVersionLock = new ReentrantReadWriteLock();
    private final LinkedHashMap<Long, TxBundle> _notFlushedBundles = new LinkedHashMap<>();

    private final Object _flushWaitSynchronizer = new Object();
    private final AtomicLong _lastWrittenTx = new AtomicLong(-1);
    private final AtomicLong _counter = new AtomicLong();
    private final AtomicLong _lastCommittedTx = new AtomicLong(-1);
    private final AtomicLong _waitedTotal = new AtomicLong(0);
    @Inject
    CachingObjectPersistentStore cachedStore;
    @ConfigProperty(name = "dhfs.objects.writeback.limit")
    long sizeLimit;
    private long currentSize = 0;
    private ExecutorService _writebackExecutor;
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
        _counter.set(cachedStore.getLastTxId());
        _lastCommittedTx.set(cachedStore.getLastTxId());
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
                        ), bundle.getId());

                Log.trace("Bundle " + bundle.getId() + " committed");

                // Remove from pending writes, after real commit
                // As we are the only writers to _pendingWrites, no need to synchronize with iterator creation
                // if they get the older version, as it will still contain all the new changes
                synchronized (_pendingBundles) {
                    var curPw = _pendingWrites.get();
                    for (var e : bundle._entries.values()) {
                        var cur = curPw.get(e.key());
                        if (cur.bundleId() <= bundle.getId())
                            curPw = curPw.minus(e.key());
                    }
                    _pendingWrites.set(curPw);
                    // No need to increment version
                }

                List<List<Runnable>> callbacks = new ArrayList<>();
                synchronized (_notFlushedBundles) {
                    _lastWrittenTx.set(bundle.getId());
                    while (!_notFlushedBundles.isEmpty() && _notFlushedBundles.firstEntry().getKey() <= bundle.getId()) {
                        callbacks.add(_notFlushedBundles.pollFirstEntry().getValue().setCommitted());
                    }
                }
                callbacks.forEach(l -> l.forEach(Runnable::run));

                synchronized (_flushWaitSynchronizer) {
                    currentSize -= bundle.calculateTotalSize();
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
                    var bundle = new TxBundle(_counter.incrementAndGet());
                    _pendingBundles.addLast(bundle);
                    _notFlushedBundles.put(bundle.getId(), bundle);
                    return bundle;
                }
            }
        }
    }

    public void commitBundle(TxBundle bundle) {
        verifyReady();
        _pendingWritesVersionLock.writeLock().lock();
        try {
            synchronized (_pendingBundles) {
                var curPw = _pendingWrites.get();
                for (var e : ((TxBundle) bundle)._entries.values()) {
                    switch (e) {
                        case TxBundle.CommittedEntry c -> {
                            curPw = curPw.plus(c.key(), new PendingWrite(c.data, bundle.getId()));
                        }
                        case TxBundle.DeletedEntry d -> {
                            curPw = curPw.plus(d.key(), new PendingDelete(d.key, bundle.getId()));
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + e);
                    }
                }
                // Now, make the changes visible to new iterators
                _pendingWrites.set(curPw);
                ((TxBundle) bundle).setReady();
                if (_pendingBundles.peek() == bundle)
                    _pendingBundles.notify();
                synchronized (_flushWaitSynchronizer) {
                    currentSize += ((TxBundle) bundle).calculateTotalSize();
                }
            }
            assert bundle.getId() > _lastCommittedTx.get();
            _lastCommittedTx.set(bundle.getId());
        } finally {
            _pendingWritesVersionLock.writeLock().unlock();
        }
    }

    public void dropBundle(TxBundle bundle) {
        verifyReady();
        synchronized (_pendingBundles) {
            Log.warn("Dropped bundle: " + bundle);
            _pendingBundles.remove((TxBundle) bundle);
            synchronized (_flushWaitSynchronizer) {
                currentSize -= ((TxBundle) bundle).calculateTotalSize();
            }
        }
    }

    public void fence(long bundleId) {
        var latch = new CountDownLatch(1);
        asyncFence(bundleId, latch::countDown);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void asyncFence(long bundleId, Runnable fn) {
        verifyReady();
        if (bundleId < 0) throw new IllegalArgumentException("txId should be >0!");
        if (_lastWrittenTx.get() >= bundleId) {
            fn.run();
            return;
        }
        synchronized (_notFlushedBundles) {
            if (_lastWrittenTx.get() >= bundleId) {
                fn.run();
                return;
            }
            _notFlushedBundles.get(bundleId).addCallback(fn);
        }
    }

    public Optional<PendingWriteEntry> getPendingWrite(JObjectKey key) {
        synchronized (_pendingBundles) {
            return Optional.ofNullable(_pendingWrites.get().get(key));
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        var pending = getPendingWrite(name).orElse(null);
        return switch (pending) {
            case PendingWrite write -> Optional.of(write.data());
            case PendingDelete ignored -> Optional.empty();
            case null -> cachedStore.readObject(name);
            default -> throw new IllegalStateException("Unexpected value: " + pending);
        };
    }

    @Nonnull
    public VerboseReadResult readObjectVerbose(JObjectKey key) {
        var pending = getPendingWrite(key).orElse(null);
        if (pending != null) {
            return new VerboseReadResultPending(pending);
        }
        return new VerboseReadResultPersisted(cachedStore.readObject(key));
    }

    /**
     * @param commitLocked - a function that will be called with a Consumer of a new transaction id,
     *                     that will commit the transaction the changes in the store will be visible to new transactions
     *                     only after the runnable is called
     */
    public Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, BiConsumer<Long, Runnable> commitLocked) {
        var bundle = createBundle();
        long bundleId = bundle.getId();
        try {
            for (var action : writes) {
                switch (action) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Flushing object " + write.key());
                        bundle.commit(new JDataVersionedWrapperImpl(write.data(), bundleId));
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
        } catch (Throwable t) {
            dropBundle(bundle);
            throw new TxCommitException(t.getMessage(), t);
        }


        Log.tracef("Committing transaction %d to storage", bundleId);
        commitLocked.accept(bundleId, () -> {
            commitBundle(bundle);
        });

        return r -> asyncFence(bundleId, r);
    }

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        PSortedMap<JObjectKey, PendingWriteEntry> pendingWrites;
        Snapshot<JObjectKey, JDataVersionedWrapper> cache = null;
        long lastTxId;

        try {
            _pendingWritesVersionLock.readLock().lock();
            try {
                pendingWrites = _pendingWrites.get();
                cache = cachedStore.getSnapshot();
                lastTxId = getLastTxId();
            } finally {
                _pendingWritesVersionLock.readLock().unlock();
            }

            Snapshot<JObjectKey, JDataVersionedWrapper> finalCache = cache;
            return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
                private final PSortedMap<JObjectKey, PendingWriteEntry> _pendingWrites = pendingWrites;
                private final Snapshot<JObjectKey, JDataVersionedWrapper> _cache = finalCache;
                private final long txId = lastTxId;

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
                    assert lastTxId >= _cache.id();
                    return lastTxId;
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

    public long getLastTxId() {
        _pendingWritesVersionLock.readLock().lock();
        try {
            return _lastCommittedTx.get();
        } finally {
            _pendingWritesVersionLock.readLock().unlock();
        }
    }

    public interface VerboseReadResult {
    }

    private static class TxBundle {
        private final LinkedHashMap<JObjectKey, BundleEntry> _entries = new LinkedHashMap<>();
        private final ArrayList<Runnable> _callbacks = new ArrayList<>();
        private long _txId;
        private volatile boolean _ready = false;
        private long _size = -1;
        private boolean _wasCommitted = false;

        private TxBundle(long txId) {
            _txId = txId;
        }

        public long getId() {
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

        public void commit(JDataVersionedWrapper obj) {
            synchronized (_entries) {
                _entries.put(obj.data().key(), new CommittedEntry(obj.data().key(), obj, obj.data().estimateSize()));
            }
        }

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

        public void compress(TxBundle other) {
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
