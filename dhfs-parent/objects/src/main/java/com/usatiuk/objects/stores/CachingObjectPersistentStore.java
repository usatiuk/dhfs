package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperLazy;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.utils.ListUtils;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class CachingObjectPersistentStore {
    private final AtomicReference<Cache> _cache;
    @Inject
    SerializingObjectPersistentStore delegate;
    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;
    private ExecutorService _commitExecutor;
    private ExecutorService _statusExecutor;
    private AtomicLong _cached = new AtomicLong();
    private AtomicLong _cacheTries = new AtomicLong();

    public CachingObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.lru.limit") int sizeLimit) {
        _cache = new AtomicReference<>(
                new Cache(TreePMap.empty(), 0, -1, sizeLimit)
        );
    }

    void init(@Observes @Priority(110) StartupEvent event) {
        try (var s = delegate.getSnapshot()) {
            _cache.set(_cache.get().withVersion(s.id()));
        }

        _commitExecutor = Executors.newSingleThreadExecutor();
        if (printStats) {
            _statusExecutor = Executors.newSingleThreadExecutor();
            _statusExecutor.submit(() -> {
                try {
                    while (true) {
                        Log.infov("Cache status: size=" + _cache.get().size() / 1024 / 1024 + "MB" + " cache success ratio: " + (_cached.get() / (double) _cacheTries.get()));
                        _cached.set(0);
                        _cacheTries.set(0);
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
    }

    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> objs, long txId) {
        Log.tracev("Committing: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());

        var cache = _cache.get();
        var commitFuture = _commitExecutor.submit(() -> delegate.prepareTx(objs, txId).run());
        for (var write : objs.written()) {
            cache = cache.withPut(write.getLeft(), Optional.of(write.getRight()));
        }
        for (var del : objs.deleted()) {
            cache = cache.withPut(del, Optional.empty());
        }
        cache = cache.withVersion(txId);
        try {
            commitFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cache.set(cache);

        Log.tracev("Committed: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());
    }

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        while (true) {
            var cache = _cache.get();

            if (cache == null)
                return delegate.getSnapshot();

            Cache curCache = null;
            Snapshot<JObjectKey, JDataVersionedWrapper> backing = null;

            try {
                curCache = _cache.get();
                backing = delegate.getSnapshot();

                if (curCache.version() != backing.id()) {
                    backing.close();
                    backing = null;
                    continue;
                }
                Snapshot<JObjectKey, JDataVersionedWrapper> finalBacking = backing;
                Cache finalCurCache = curCache;
                return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
                    private final Cache _curCache = finalCurCache;
                    private final Snapshot<JObjectKey, JDataVersionedWrapper> _backing = finalBacking;
                    private boolean _invalid = false;
                    private boolean _closed = false;

                    private void doCache(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
                        _cacheTries.incrementAndGet();
                        if (_invalid)
                            return;

                        var globalCache = _cache.get();
                        if (globalCache.version() != _curCache.version()) {
                            _invalid = true;
                            return;
                        }

                        var newCache = globalCache.withPut(key, obj);
                        if (_cache.compareAndSet(globalCache, newCache))
                            _cached.incrementAndGet();
                    }

                    private void maybeCache(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
                        if (obj.isEmpty()) {
                            doCache(key, obj);
                            return;
                        }

                        var wrapper = obj.get();

                        if (!(wrapper instanceof JDataVersionedWrapperLazy lazy)) {
                            doCache(key, obj);
                            return;
                        }

                        lazy.setCacheCallback(() -> {
                            if (_closed) {
                                Log.error("Cache callback called after close");
                                System.exit(-1);
                            }
                            doCache(key, obj);
                        });
                        return;
                    }

                    @Override
                    public List<CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>> getIterator(IteratorStart start, JObjectKey key) {
                        return ListUtils.prependAndMap(
                                new NavigableMapKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>(_curCache.map(), start, key),
                                _backing.getIterator(start, key),
                                i -> new CachingKvIterator((CloseableKvIterator<JObjectKey, JDataVersionedWrapper>) (CloseableKvIterator<JObjectKey, ?>) i)
                        );
                    }

                    @Nonnull
                    @Override
                    public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                        var cached = _curCache.map().get(name);
                        if (cached != null) {
                            return switch (cached) {
                                case CacheEntryPresent data -> Optional.of(data.value());
                                case CacheEntryMiss tombstone -> {
                                    yield Optional.empty();
                                }
                                default -> throw new IllegalStateException("Unexpected value: " + cached);
                            };
                        }
                        var read = _backing.readObject(name);
                        maybeCache(name, read);
                        return read;
                    }

                    @Override
                    public long id() {
                        return _backing.id();
                    }

                    @Override
                    public void close() {
                        _closed = true;
                        _backing.close();
                    }

                    private class CachingKvIterator implements CloseableKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> {
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
                        public Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> prev() {
                            var prev = _delegate.prev();
                            maybeCache(prev.getKey(), Optional.of(prev.getValue()));
                            return (Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>) (Pair<JObjectKey, ?>) prev;
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
                        public Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> next() {
                            var next = _delegate.next();
                            maybeCache(next.getKey(), Optional.of(next.getValue()));
                            return (Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>) (Pair<JObjectKey, ?>) next;
                        }
                    }
                };
            } catch (Throwable ex) {
                if (backing != null) {
                    backing.close();
                }
                throw ex;
            }
        }
    }

    private interface CacheEntry extends MaybeTombstone<JDataVersionedWrapper> {
        int size();
    }

    private record Cache(TreePMap<JObjectKey, CacheEntry> map,
                         int size,
                         long version,
                         int sizeLimit) {
        public Cache withPut(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
            var entry = obj.<CacheEntry>map(o -> new CacheEntryPresent(o, o.estimateSize())).orElse(new CacheEntryMiss());

            int newSize = size() + entry.size();

            var old = map.get(key);
            if (old != null)
                newSize -= old.size();

            TreePMap<JObjectKey, CacheEntry> newCache = map();

            while (newSize > sizeLimit) {
                var del = newCache.firstEntry();
                newCache = newCache.minusFirstEntry();
                newSize -= del.getValue().size();
            }

            newCache = newCache.plus(key, entry);
            return new Cache(
                    newCache,
                    newSize,
                    version,
                    sizeLimit
            );
        }

        public Cache withVersion(long version) {
            return new Cache(map, size, version, sizeLimit);
        }
    }

    private record CacheEntryPresent(JDataVersionedWrapper value,
                                     int size) implements CacheEntry, Data<JDataVersionedWrapper> {
    }

    private record CacheEntryMiss() implements CacheEntry, Tombstone<JDataVersionedWrapper> {
        @Override
        public int size() {
            return 64;
        }
    }
}
