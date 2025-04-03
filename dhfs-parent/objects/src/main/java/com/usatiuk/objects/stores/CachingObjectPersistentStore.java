package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class CachingObjectPersistentStore {
    @Inject
    SerializingObjectPersistentStore delegate;
    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;

    private record Cache(TreePMap<JObjectKey, CacheEntry> map,
                         int size,
                         long version,
                         int sizeLimit) {
        public Cache withPut(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
            int objSize = obj.map(JDataVersionedWrapper::estimateSize).orElse(16);

            int newSize = size() + objSize;
            var entry = new CacheEntry(obj.<MaybeTombstone<JDataVersionedWrapper>>map(Data::new).orElse(new Tombstone<>()), objSize);

            var old = map.get(key);
            if (old != null)
                newSize -= old.size();

            TreePMap<JObjectKey, CacheEntry> newCache = map().plus(key, entry);

            while (newSize > sizeLimit) {
                var del = newCache.firstEntry();
                newCache = newCache.minusFirstEntry();
                newSize -= del.getValue().size();
            }
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

    private final AtomicReference<Cache> _cache;
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
                    private boolean _invalid = false;
                    private final Cache _curCache = finalCurCache;
                    private final Snapshot<JObjectKey, JDataVersionedWrapper> _backing = finalBacking;

                    private void maybeCache(JObjectKey key, Optional<JDataVersionedWrapper> obj) {
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

                    @Override
                    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
                        return new TombstoneMergingKvIterator<>("cache", start, key,
                                (mS, mK)
                                        -> new MappingKvIterator<>(
                                        new NavigableMapKvIterator<>(_curCache.map(), mS, mK),
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
                        var cached = _curCache.map().get(name);
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
                };
            } catch (Throwable ex) {
                if (backing != null) {
                    backing.close();
                }
                throw ex;
            }
        }
    }

    private record CacheEntry(MaybeTombstone<JDataVersionedWrapper> object, int size) {
    }
}
