package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.utils.DataLocker;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@ApplicationScoped
public class CachingObjectPersistentStore {
    private final LinkedHashMap<JObjectKey, CacheEntry> _cache = new LinkedHashMap<>(8, 0.75f, true);
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

    private void put(JObjectKey key, Optional<JDataVersionedWrapper<?>> obj) {
        synchronized (_cache) {
            int size = obj.map(o -> o.data().estimateSize()).orElse(0);

            _curSize += size;
            var old = _cache.putLast(key, new CacheEntry(obj, size));
            if (old != null)
                _curSize -= old.size();

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue().size();
                _evict++;
            }
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper<?>> readObject(JObjectKey name) {
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

    public void writeObject(JObjectKey name, JDataVersionedWrapper<?> object) {
        delegate.writeObject(name, object);
    }

    public void commitTx(TxManifest names) {
        // During commit, readObject shouldn't be called for these items,
        // it should be handled by the upstream store
        synchronized (_cache) {
            for (var key : Stream.concat(names.written().stream(), names.deleted().stream()).toList()) {
                _curSize -= Optional.ofNullable(_cache.get(key)).map(CacheEntry::size).orElse(0L);
                _cache.remove(key);
            }
        }
        delegate.commitTx(names);
    }

    private record CacheEntry(Optional<JDataVersionedWrapper<?>> object, long size) {
    }
}
