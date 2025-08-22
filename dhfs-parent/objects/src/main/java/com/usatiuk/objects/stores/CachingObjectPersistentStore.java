package com.usatiuk.objects.stores;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperLazy;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;
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

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * CachingObjectPersistentStore is a caching layer for the SerializingObjectPersistentStore
 * It stores the already deserialized objects in memory.
 * Not (yet) thread safe for writes.
 */
@ApplicationScoped
public class CachingObjectPersistentStore {
    @Inject
    SerializingObjectPersistentStore delegate;
    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;
//    private ExecutorService _statusExecutor;

    private final com.github.benmanes.caffeine.cache.Cache<Pair<Long, JObjectKey>, JDataVersionedWrapper> _cache;

    public CachingObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.lru.limit") int sizeLimit) {
        _cache = Caffeine.newBuilder()
                .maximumWeight(sizeLimit)
                .weigher((Pair<Long, JObjectKey> key, JDataVersionedWrapper value) -> value.estimateSize())
                .expireAfterWrite(Duration.ofMinutes(5)).build();
    }

    void init(@Observes @Priority(110) StartupEvent event) {
//        if (printStats) {
//            _statusExecutor = Executors.newSingleThreadExecutor();
//            _statusExecutor.submit(() -> {
//                try {
//                    while (true) {
//                        Log.infov("Cache status: size=" + _cache.estimatedSize() / 1024 / 1024 + "MB" + " cache success ratio: " + (_cached.get() / (double) _cacheTries.get()));
//                        _cached.set(0);
//                        _cacheTries.set(0);
//                        Thread.sleep(1000);
//                    }
//                } catch (InterruptedException ignored) {
//                }
//            });
//        }
    }

    /**
     * Commit the transaction to the underlying store and update the cache.
     * Once this function returns, the transaction is committed and the cache is updated.
     *
     * @param objs the transaction manifest object
     * @param txId the transaction ID
     */
    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> objs, long txId) {
        Log.tracev("Committing: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());

        delegate.commitTx(objs, txId);

        Log.tracev("Committed: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());
    }

    /**
     * Get a snapshot of underlying store and the cache.
     * Objects are read from the cache if possible, if not, they are read from the underlying store,
     * then possibly lazily cached when their data is accessed.
     *
     * @return a snapshot of the cached store
     */
    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        while (true) {
            Snapshot<JObjectKey, JDataVersionedWrapper> backing = null;

            try {
                backing = delegate.getSnapshot();

                Snapshot<JObjectKey, JDataVersionedWrapper> finalBacking = backing;
                return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
                    private final Snapshot<JObjectKey, JDataVersionedWrapper> _backing = finalBacking;
                    private boolean _closed = false;

                    private void doCache(JObjectKey key, JDataVersionedWrapper obj) {
                        var cacheKey = Pair.of(obj.version(), key);
                        _cache.put(cacheKey, obj);
                    }

                    private void maybeCache(JObjectKey key, JDataVersionedWrapper obj) {
                        if (!(obj instanceof JDataVersionedWrapperLazy lazy)) {
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
                        return ListUtils.map(
                                _backing.getIterator(start, key),
                                i -> new CachingKvIterator((CloseableKvIterator<JObjectKey, JDataVersionedWrapper>) (CloseableKvIterator<JObjectKey, ?>) i)
                        );
                    }

                    private JDataVersionedWrapper tryGetCached(JObjectKey key, JDataVersionedWrapper obj) {
                        var cached = _cache.getIfPresent(Pair.of(obj.version(), key));
                        if (cached != null) {
                            return cached;
                        }
                        maybeCache(key, obj);
                        return obj;
                    }

                    @Nonnull
                    @Override
                    public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                        return _backing.readObject(name).map(o -> tryGetCached(name, o));
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
                            var cached = tryGetCached(prev.getKey(), prev.getValue());
                            return Pair.of(prev.getKey(), cached);
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
                            var cached = tryGetCached(next.getKey(), next.getValue());
                            return Pair.of(next.getKey(), cached);
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
}
