package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.utils.DataLocker;
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
    private final DataLocker _readerLocker = new DataLocker();

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
            CacheEntry entry = obj.<CacheEntry>map(v -> new CacheEntryYes(v, size)).orElse(new CacheEntryDeleted());
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
                return switch (got) {
                    case CacheEntryYes yes -> Optional.of(yes.object());
                    case CacheEntryDeleted del -> Optional.empty();
                    default -> throw new IllegalStateException("Unexpected value: " + got);
                };
            }
        } finally {
            _lock.readLock().unlock();
        }
        try (var lock = _readerLocker.lock(name)) {
            // TODO: This is possibly racy
//            var got = delegate.readObject(name);
//            put(name, got);
            return delegate.readObject(name);
        }
    }

    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> names, long txId) {
        var serialized = delegate.prepareManifest(names);
        Log.tracev("Committing: {0} writes, {1} deletes", names.written().size(), names.deleted().size());
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


    private class CachingKvIterator implements CloseableKvIterator<JObjectKey, JDataVersionedWrapper> {
        private final CloseableKvIterator<JObjectKey, JDataVersionedWrapper> _delegate;
        // This should be created under lock
        private final long _curCacheVersion = _cacheVersion;

        private CachingKvIterator(CloseableKvIterator<JObjectKey, JDataVersionedWrapper> delegate) {
            _delegate = delegate;
        }

        @Override
        public JObjectKey peekNextKey() {
            return _delegate.peekNextKey();
        }

        @Override
        public Class<?> peekNextType() {
            return _delegate.peekNextType();
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
        public Class<?> peekPrevType() {
            return _delegate.peekPrevType();
        }

        private void maybeCache(Pair<JObjectKey, JDataVersionedWrapper> prev) {
            _lock.writeLock().lock();
            try {
                if (_cacheVersion != _curCacheVersion) {
                    Log.tracev("Not caching: {0}", prev);
                } else {
                    Log.tracev("Caching: {0}", prev);
                    put(prev.getKey(), Optional.of(prev.getValue()));
                }
            } finally {
                _lock.writeLock().unlock();
            }
        }

        @Override
        public Pair<JObjectKey, JDataVersionedWrapper> prev() {
            var prev = _delegate.prev();
            maybeCache(prev);
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
            maybeCache(next);
            return next;
        }
    }

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    // Warning: it has a nasty side effect of global caching, so in this case don't even call next on it,
    // if some objects are still in writeback
    public CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> getIterator(IteratorStart start, JObjectKey key) {
        _lock.readLock().lock();
        try {
            Log.tracev("Getting cache iterator: {0}, {1}", start, key);
            var curSortedCache = _sortedCache;
            return new MergingKvIterator<>("cache", start, key,
                    (mS, mK)
                            -> new MappingKvIterator<>(
                            new NavigableMapKvIterator<>(curSortedCache, mS, mK),
                            e -> switch (e) {
                                case CacheEntryYes pw -> new Data<>(pw.object());
                                case CacheEntryDeleted d -> new Tombstone<>();
                                default -> throw new IllegalStateException("Unexpected value: " + e);
                            },
                            e -> {
                                if (CacheEntryYes.class.isAssignableFrom(e)) {
                                    return Data.class;
                                } else if (CacheEntryDeleted.class.isAssignableFrom(e)) {
                                    return Tombstone.class;
                                } else {
                                    throw new IllegalStateException("Unexpected type: " + e);
                                }
                            }),
                    (mS, mK)
                            -> new MappingKvIterator<>(new CachingKvIterator(delegate.getIterator(mS, mK)), Data::new, (d) -> Data.class));
        } finally {
            _lock.readLock().unlock();
        }
    }

    private interface CacheEntry {
        long size();
    }

    private record CacheEntryYes(JDataVersionedWrapper object, long size) implements CacheEntry {
    }

    private record CacheEntryDeleted() implements CacheEntry {
        @Override
        public long size() {
            return 16;
        }
    }

    public long getLastTxId() {
        return delegate.getLastCommitId();
    }
}
