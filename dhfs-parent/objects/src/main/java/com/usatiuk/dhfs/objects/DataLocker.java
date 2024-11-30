package com.usatiuk.dhfs.objects;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class DataLocker {
    private final ConcurrentHashMap<JObjectKey, WeakReference<? extends LockWrapper<? extends JData>>> _locks = new ConcurrentHashMap<>();
    private final static Cleaner CLEANER = Cleaner.create();

    public <T extends JData> LockWrapper<T> get(T data) {
        while (true) {
            var have = _locks.get(data.getKey());
            if (have != null) {
                var ret = have.get();
                if (ret != null) {
                    if (ret.sameObject(data)) {
                        return (LockWrapper<T>) ret;
                    } else {
                        Log.warn("Removed stale lock for " + data.getKey());
                        _locks.remove(data.getKey(), have);
                    }
                }
            }

            var ret = new LockWrapper<>(data);
            var ref = new WeakReference<>(ret);

            if (_locks.putIfAbsent(data.getKey(), ref) == null) {
                CLEANER.register(ret, () -> _locks.remove(data.getKey(), ref));
                return ret;
            }
        }
    }

}
