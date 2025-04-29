package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JDataVersionedWrapperLazy;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.Data;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.utils.ListUtils;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Nullable;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class CachingObjectPersistentStore {
    @Inject
    SerializingObjectPersistentStore delegate;

    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;
    @ConfigProperty(name = "dhfs.objects.lru.limit")
    int sizeLimit;

    private ExecutorService _commitExecutor;
    private ExecutorService _statusExecutor;

    private final LinkedHashMap<JObjectKey, JDataVersionedWrapper> _cache = new LinkedHashMap<>(8, 0.75f, true);
    private long _curSize = 0;

    private void put(JObjectKey key, JDataVersionedWrapper obj) {
        synchronized (_cache) {
            int size = obj.estimateSize();

            _curSize += size;
            var old = _cache.putLast(key, obj);
            if (old != null)
                _curSize -= old.estimateSize();

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue().estimateSize();
//                _evict++;
            }
        }
    }

    private @Nullable JDataVersionedWrapper get(JObjectKey key) {
        synchronized (_cache) {
            return _cache.get(key);
        }
    }


    void init(@Observes @Priority(110) StartupEvent event) {
        _commitExecutor = Executors.newSingleThreadExecutor();
        if (printStats) {
            _statusExecutor = Executors.newSingleThreadExecutor();
            _statusExecutor.submit(() -> {
                try {
                    while (true) {
                        Log.infov("Cache status: size=" + _curSize / 1024 / 1024 + "MB");
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
    }

    public void commitTx(TxManifestObj<? extends JDataVersionedWrapper> objs, long txId) {
        Log.tracev("Committing: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());

        var commitFuture = _commitExecutor.submit(
                () -> delegate
                        .prepareTx(objs, txId)
                        .run()
        );
        for (var write : objs.written()) {
            put(write.getKey(), write.getValue());
        }
        try {
            commitFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Log.tracev("Committed: {0} writes, {1} deletes", objs.written().size(), objs.deleted().size());
    }

    public Snapshot<JObjectKey, JDataVersionedWrapper> getSnapshot() {
        return new Snapshot<JObjectKey, JDataVersionedWrapper>() {
            private final Snapshot<JObjectKey, JDataVersionedWrapper> _backing = delegate.getSnapshot();
            private boolean _invalid = false;
            private boolean _closed = false;

            private void doCache(JObjectKey key, JDataVersionedWrapper obj) {
                if (_invalid)
                    return;
                put(key, obj);
            }

            private void maybeCache(JObjectKey key, JDataVersionedWrapper obj) {
                if (_invalid) {
                    return;
                }

                if (!(obj instanceof JDataVersionedWrapperLazy lazy)) {
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
                return ListUtils.map(_backing.getIterator(start, key),
                        i -> new CachingKvIterator((CloseableKvIterator<JObjectKey, JDataVersionedWrapper>) (CloseableKvIterator<JObjectKey, ?>) i)
                );
            }

            private Optional<JDataVersionedWrapper> tryGet(JObjectKey key) {
                var cached = get(key);
                if (cached != null) {
                    if (cached.version() > _backing.id()) {
                        _invalid = true;
                    } else {
                        return Optional.of(cached);
                    }
                }
                return Optional.empty();
            }

            @Nonnull
            @Override
            public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
                return tryGet(name).or(() -> {
                    var read = _backing.readObject(name);
                    read.ifPresent(r -> maybeCache(name, r));
                    return read;
                });
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
                    if (prev.getValue() instanceof Data<JDataVersionedWrapper> data) {
                        var tried = tryGet(prev.getKey());
                        if (tried.isPresent()) {
                            return Pair.of(prev.getKey(), tried.get());
                        }
                        maybeCache(prev.getKey(), data.value());
                    }
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
                    if (next.getValue() instanceof Data<JDataVersionedWrapper> data) {
                        var tried = tryGet(next.getKey());
                        if (tried.isPresent()) {
                            return Pair.of(next.getKey(), tried.get());
                        }
                        maybeCache(next.getKey(), data.value());
                    }
                    return (Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>>) (Pair<JObjectKey, ?>) next;
                }
            }
        };
    }
}
