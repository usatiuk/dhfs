package com.usatiuk.dhfs.objects.jrepository;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class JObjectLRU {
    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;
    @ConfigProperty(name = "dhfs.objects.lru.limit")
    long sizeLimit;

    private long _curSize = 0;
    private long _evict = 0;

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
        var newSize = jObjectSizeEstimator.estimateObjectSize(obj.getData());
        synchronized (this) {
            long oldSize = _cache.getOrDefault(obj, 0L);
            _curSize -= oldSize;
            _curSize += newSize;
            _cache.putLast(obj, newSize);

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue();
                _evict++;
            }
        }
    }
}
