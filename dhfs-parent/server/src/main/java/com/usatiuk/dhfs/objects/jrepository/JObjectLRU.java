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
    @ConfigProperty(name = "dhfs.objects.lru.print-stats")
    boolean printStats;

    private long _curSize = 0;
    private long _evict = 0;

    private final LinkedHashMap<JObject<?>, Long> _cache = new LinkedHashMap<>();
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
                            Log.info("Cache status: size="
                                    + _curSize / 1024 / 1024 + "MB"
                                    + " evicted=" + _evict);
                        _evict = 0;
                        if (Log.isTraceEnabled()) {
                            long realSize = 0;
                            synchronized (_cache) {
                                for (JObject<?> object : _cache.keySet()) {
                                    realSize += jObjectSizeEstimator.estimateObjectSize(object.getData());
                                }
                                Log.info("Cache status: real size="
                                        + realSize / 1024 / 1024 + "MB" + " entries=" + _cache.size());
                            }
                        }
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
    }

    @Shutdown
    void shutdown() {
        if (_statusExecutor != null)
            _statusExecutor.shutdownNow();
    }

    // Called in JObjectManager getters to add into cache,
    // and when resolving/modifying to update size
    public void notifyAccess(JObject<?> obj) {
        if (obj.getData() == null) return;
        long size = jObjectSizeEstimator.estimateObjectSize(obj.getData());
        synchronized (_cache) {
            _curSize += size;
            var old = _cache.putLast(obj, size);
            if (old != null)
                _curSize -= old;

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue();
                _evict++;
            }
        }
    }
}
