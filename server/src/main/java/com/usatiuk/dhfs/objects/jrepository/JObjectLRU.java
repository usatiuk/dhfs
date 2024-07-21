package com.usatiuk.dhfs.objects.jrepository;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class JObjectLRU {
    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;
    @ConfigProperty(name = "dhfs.objects.lru.limit")
    long sizeLimit;

    private long _curSize = 0;
    private long _evict = 0;

    private final AtomicReference<ConcurrentHashMap<JObject<?>, Long>> _accessQueue = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicLong _lastDrain = new AtomicLong(0);

    private final LinkedHashMap<JObject<?>, Long> _cache = new LinkedHashMap<>();
    private ExecutorService _statusExecutor;

    @Startup
    void init() {
        _statusExecutor = Executors.newSingleThreadExecutor();
        _statusExecutor.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    if (_curSize > 0)
                        Log.info("Cache status: size="
                                + _curSize / 1024 / 1024 + "MB"
                                + " evicted=" + _evict);
                    _evict = 0;
                }
            } catch (InterruptedException ignored) {
            }
        });
    }

    @Shutdown
    void shutdown() {
        _statusExecutor.shutdownNow();
    }

    public void notifyAccess(JObject<?> obj) {
        _accessQueue.get().put(obj, jObjectSizeEstimator.estimateObjectSize(obj.getData()));
        // TODO: no hardcoding
        if (_accessQueue.get().size() > 500 || System.currentTimeMillis() - _lastDrain.get() > 100) {
            synchronized (_cache) {
                _lastDrain.set(System.currentTimeMillis());
                var newQueue = new ConcurrentHashMap<JObject<?>, Long>();
                var oldQueue = _accessQueue.getAndSet(newQueue);

                for (var x : oldQueue.entrySet()) {
                    long oldSize = _cache.getOrDefault(x.getKey(), 0L);
                    long newSize = x.getValue();
                    _curSize -= oldSize;
                    _curSize += newSize;
                    _cache.putLast(x.getKey(), newSize);

                    while (_curSize >= sizeLimit) {
                        var del = _cache.pollFirstEntry();
                        _curSize -= del.getValue();
                        _evict++;
                    }
                }
            }
        }
    }
}
