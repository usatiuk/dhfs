package com.usatiuk.dhfs.objects.jrepository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;

@ApplicationScoped
public class JObjectLRU {

    @Inject
    JObjectSizeEstimator jObjectSizeEstimator;
    @ConfigProperty(name = "dhfs.objects.lru.limit")
    long sizeLimit;

    private long _curSize = 0;

    private final LinkedHashMap<JObject<?>, Long> _cache = new LinkedHashMap<>();

    public void notifyAccess(JObject<?> obj) {
        synchronized (this) {
            var newSize = jObjectSizeEstimator.estimateObjectSize(obj.getData());
            if (_cache.containsKey(obj)) {
                var oldSize = _cache.get(obj);
                _curSize -= oldSize;
                _curSize += newSize;
            } else {
                _curSize += newSize;
            }
            _cache.putLast(obj, newSize);

            while (_curSize >= sizeLimit) {
                var del = _cache.pollFirstEntry();
                _curSize -= del.getValue();
            }
        }
    }
}
