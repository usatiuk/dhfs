package com.usatiuk.dhfs.objects.jrepository;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class JObjectLRU {
    private final LinkedHashMap<JObject<?>, Long> _cache = new LinkedHashMap<>();
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
                            Log.info("Cache status: size="
                                    + _curSize / 1024 / 1024 + "MB"
                                    + " evicted=" + _evict);
                        _evict = 0;
                        if (Log.isTraceEnabled()) {
                            long realSize = 0;
                            synchronized (_cache) {
                                for (JObject<?> object : _cache.keySet()) {
                                    realSize += object.estimateSize();
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

    public void notifyAccess(JObject<?> obj) {
        if (obj.getData() == null) return;
        long size = obj.estimateSize();
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

    public void updateSize(JObject<?> obj) {
        long size = obj.estimateSize();
        synchronized (_cache) {
            var old = _cache.replace(obj, size);
            if (old != null) {
                _curSize += size;
                _curSize -= old;
            } else {
                return;
            }

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue();
                _evict++;
            }
        }
    }
}
