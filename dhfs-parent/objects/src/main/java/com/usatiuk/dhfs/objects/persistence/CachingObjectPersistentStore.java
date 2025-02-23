package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.utils.DataLocker;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@ApplicationScoped
public class CachingObjectPersistentStore {
    private final LinkedHashMap<JObjectKey, CacheEntry> _cache = new LinkedHashMap<>(8, 0.75f, true);
    private final ConcurrentSkipListMap<JObjectKey, CacheEntry> _sortedCache = new ConcurrentSkipListMap<>();
    private final HashSet<JObjectKey> _pendingWrites = new HashSet<>();
    private final DataLocker _locker = new DataLocker();
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

    @Nonnull
    public Collection<JObjectKey> findAllObjects() {
        return delegate.findAllObjects();
    }

    private void put(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
//        Log.tracev("Adding {0} to cache: {1}", key, obj);
        synchronized (_cache) {
            assert !_pendingWrites.contains(key);
            int size = obj.map(o -> o.data().estimateSize()).orElse(0);

            _curSize += size;
            var entry = new CacheEntry(obj, size);
            var old = _cache.putLast(key, entry);
            _sortedCache.put(key, entry);
            if (old != null)
                _curSize -= old.size();

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _sortedCache.remove(del.getKey(), del.getValue());
                _curSize -= del.getValue().size();
                _evict++;
            }
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        try (var lock = _locker.lock(name)) {
            synchronized (_cache) {
                var got = _cache.get(name);
                if (got != null) {
                    return got.object();
                }
            }

            var got = delegate.readObject(name);
            put(name, got);
            return got;
        }
    }

    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> names) {
        // During commit, readObject shouldn't be called for these items,
        // it should be handled by the upstream store
        synchronized (_cache) {
            for (var key : Stream.concat(names.written().stream().map(Pair::getLeft),
                    names.deleted().stream()).toList()) {
                _curSize -= Optional.ofNullable(_cache.get(key)).map(CacheEntry::size).orElse(0L);
                _cache.remove(key);
                _sortedCache.remove(key);
//                Log.tracev("Removing {0} from cache", key);
                var added = _pendingWrites.add(key);
                assert added;
            }
        }
        delegate.commitTx(names);
        // Now, reading from the backing store should return the new data
        synchronized (_cache) {
            for (var key : Stream.concat(names.written().stream().map(Pair::getLeft),
                    names.deleted().stream()).toList()) {
                var removed = _pendingWrites.remove(key);
                assert removed;
            }
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
        public void close() {
            _delegate.close();
        }

        @Override
        public boolean hasNext() {
            return _delegate.hasNext();
        }

        @Override
        public Pair<JObjectKey, JDataVersionedWrapper> next() {
            var next = _delegate.next();
            put(next.getKey(), Optional.of(next.getValue()));
            return next;
        }
    }

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    // Warning: it has a nasty side effect of global caching, so in this case don't even call next on it,
    // if some objects are still in writeback
    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
        return new MergingKvIterator<>(
                new PredicateKvIterator<>(
                        new NavigableMapKvIterator<>(_sortedCache, start, key),
                        e -> e.object().orElse(null)
                ), new CachingKvIterator(delegate.getIterator(start, key)));
    }

    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }

    private record CacheEntry(Optional<JDataVersionedWrapper> object, long size) {
    }
}
