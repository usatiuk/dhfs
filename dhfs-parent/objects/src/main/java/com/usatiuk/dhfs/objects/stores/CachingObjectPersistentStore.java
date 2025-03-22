package com.usatiuk.dhfs.objects.stores;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.*;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;
import com.usatiuk.dhfs.objects.transaction.LockManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ApplicationScoped
public class CachingObjectPersistentStore {
    private final LinkedHashMap<JObjectKey, CacheEntry> _cache = new LinkedHashMap<>();
    private TreePMap<JObjectKey, CacheEntry> _sortedCache = TreePMap.empty();
    private long _cacheVersion = 0;

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    @Inject
    LockManager lockManager;

    @Inject
    SerializingObjectPersistentStore delegate;
    @ConfigProperty(name = "dhfs.objects.lru.limit")
    long sizeLimit;
    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;

    private long _curSize = 0;
    private long _evict = 0;

    private ExecutorService _statusExecutor = null;

    @Startup
    void init() {
        if (printStats) {
            _statusExecutor = Executors.newSingleThreadExecutor();
            _statusExecutor.submit(() -> {
                try {
                    while (true) {
                        Thread.sleep(10000);
                        if (_curSize > 0)
                            Log.info("Cache status: size=" + _curSize / 1024 / 1024 + "MB" + " evicted=" + _evict);
                        _evict = 0;
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
    }

    private void put(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
//        Log.tracev("Adding {0} to cache: {1}", key, obj);
        _lock.writeLock().lock();
        try {
            int size = obj.map(JDataVersionedWrapper::estimateSize).orElse(16);

            _curSize += size;
            var entry = new CacheEntry(obj.<MaybeTombstone<JDataVersionedWrapper>>map(Data::new).orElse(new Tombstone<>()), size);
            var old = _cache.putLast(key, entry);

            _sortedCache = _sortedCache.plus(key, entry);
            if (old != null)
                _curSize -= old.size();

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _sortedCache = _sortedCache.minus(del.getKey());
                _curSize -= del.getValue().size();
                _evict++;
            }
        } finally {
            _lock.writeLock().unlock();
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        _lock.readLock().lock();
        try {
            var got = _cache.get(name);
            if (got != null) {
                return got.object().opt();
            }
        } finally {
            _lock.readLock().unlock();
        }
        // Global object lock, prevent putting the object into cache
        // if its being written right now by another thread
        try (var lock = lockManager.tryLockObject(name)) {
            var got = delegate.readObject(name);

            if (lock == null)
                return got;

            _lock.writeLock().lock();
            try {
                put(name, got);
                // No need to increase cache version, the objects didn't change
            } finally {
                _lock.writeLock().unlock();
            }
            return got;
        }
    }

    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> names, long txId) {
        var serialized = delegate.prepareManifest(names);

        Log.tracev("Committing: {0} writes, {1} deletes", names.written().size(), names.deleted().size());
        // A little complicated locking to minimize write lock holding time
        delegate.commitTx(serialized, txId, (commit) -> {
            _lock.writeLock().lock();
            try {
                // Make the changes visible atomically both in cache and in the underlying store
                for (var write : names.written()) {
                    put(write.getLeft(), Optional.of(write.getRight()));
                }
                for (var del : names.deleted()) {
                    put(del, Optional.empty());
                }
                ++_cacheVersion;
                commit.run();
            } finally {
                _lock.writeLock().unlock();
            }
        });
        Log.tracev("Committed: {0} writes, {1} deletes", names.written().size(), names.deleted().size());
    }

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        TreePMap<JObjectKey, CacheEntry> curSortedCache;
        Snapshot<JObjectKey, JDataVersionedWrapper> backing = null;
        long cacheVersion;

        try {
//            Log.tracev("Getting cache snapshot");
            // Decrease the lock time as much as possible
            _lock.readLock().lock();
            try {
                curSortedCache = _sortedCache;
                cacheVersion = _cacheVersion;
                // TODO: Could this be done without lock?
                backing = delegate.getSnapshot();
            } finally {
                _lock.readLock().unlock();
            }

            Snapshot<JObjectKey, JDataVersionedWrapper> finalBacking = backing;
            return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
                private final TreePMap<JObjectKey, CacheEntry> _curSortedCache = curSortedCache;
                private final Snapshot<JObjectKey, JDataVersionedWrapper> _backing = finalBacking;
                private final long _snapshotCacheVersion = cacheVersion;

                private void maybeCache(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
                    if (_snapshotCacheVersion != _cacheVersion)
                        return;

                    _lock.writeLock().lock();
                    try {
                        if (_snapshotCacheVersion != _cacheVersion) {
//                            Log.tracev("Not caching: {0}", key);
                        } else {
//                            Log.tracev("Caching: {0}", key);
                            put(key, obj);
                        }
                    } finally {
                        _lock.writeLock().unlock();
                    }
                }

                private class CachingKvIterator implements CloseableKvIterator<JObjectKey, JDataVersionedWrapper> {
                    private final CloseableKvIterator<JObjectKey, JDataVersionedWrapper> _delegate;

                    private CachingKvIterator(CloseableKvIterator<JObjectKey, JDataVersionedWrapper> delegate) {
                        _delegate = delegate;
                    }

                    @Override
                    public JObjectKey peekNextKey() {
                        return _delegate.peekNextKey();
                    }

                    @Override
                    public void skip() {
                        _delegate.skip();
                    }

                    @Override
                    public void close() {
                        _delegate.close();
                    }

                    @Override
                    public boolean hasNext() {
                        return _delegate.hasNext();
                    }

                    @Override
                    public JObjectKey peekPrevKey() {
                        return _delegate.peekPrevKey();
                    }

                    @Override
                    public Pair<JObjectKey, JDataVersionedWrapper> prev() {
                        var prev = _delegate.prev();
                        maybeCache(prev.getKey(), Optional.of(prev.getValue()));
                        return prev;
                    }

                    @Override
                    public boolean hasPrev() {
                        return _delegate.hasPrev();
                    }

                    @Override
                    public void skipPrev() {
                        _delegate.skipPrev();
                    }

                    @Override
                    public Pair<JObjectKey, JDataVersionedWrapper> next() {
                        var next = _delegate.next();
                        maybeCache(next.getKey(), Optional.of(next.getValue()));
                        return next;
                    }
                }

                @Override
                public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
                    return new TombstoneMergingKvIterator<>("cache", start, key,
                            (mS, mK)
                                    -> new MappingKvIterator<>(
                                    new NavigableMapKvIterator<>(_curSortedCache, mS, mK),
                                    e -> {
//                                        Log.tracev("Taken from cache: {0}", e);
                                        return e.object();
                                    }
                            ),
                            (mS, mK) -> new MappingKvIterator<>(new CachingKvIterator(_backing.getIterator(start, key)), Data::new));
                }

                @Nonnull
                @Override
                public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                    var cached = _curSortedCache.get(name);
                    if (cached != null) {
                        return switch (cached.object()) {
                            case Data<JDataVersionedWrapper> data -> Optional.of(data.value());
                            case Tombstone<JDataVersionedWrapper> tombstone -> {
                                yield Optional.empty();
                            }
                            default -> throw new IllegalStateException("Unexpected value: " + cached.object());
                        };
                    }
                    var read = _backing.readObject(name);
                    maybeCache(name, read);
                    return _backing.readObject(name);
                }

                @Override
                public long id() {
                    return _backing.id();
                }

                @Override
                public void close() {
                    _backing.close();
                }
            };
        } catch (Throwable ex) {
            if (backing != null) {
                backing.close();
            }
            throw ex;
        }
    }

    private record CacheEntry(MaybeTombstone<JDataVersionedWrapper> object, long size) {
    }

    public long getLastTxId() {
        return delegate.getLastCommitId();
    }
}
