package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.files.objects.ChunkData;
import com.usatiuk.dhfs.storage.files.objects.ChunkInfo;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Getter;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Optional;

@ApplicationScoped
public class JObjectManagerImpl implements JObjectManager {
    @Inject
    JObjectRepository jObjectRepository;

    private static class NamedSoftReference extends SoftReference<JObject> {
        public NamedSoftReference(JObject target, ReferenceQueue<? super JObject> q) {
            super(target, q);
            this._key = target.getName();
        }

        @Getter
        final String _key;
    }

    private final HashMap<String, NamedSoftReference> _map = new HashMap<>();
    private final ReferenceQueue<JObject> _refQueue = new ReferenceQueue<>();

    private void cleanup() {
        NamedSoftReference cur;
        while ((cur = (NamedSoftReference) _refQueue.poll()) != null) {
            synchronized (_map) {
                if (_map.containsKey(cur._key) && (_map.get(cur._key).get() == null))
                    _map.remove(cur._key);
            }
        }
    }

    private <T extends JObject> T getFromMap(String key, Class<T> clazz) {
        synchronized (_map) {
            if (_map.containsKey(key)) {
                var ref = _map.get(key).get();
                if (ref != null) {
                    if (!clazz.isAssignableFrom(ref.getClass())) {
                        Log.error("Cached object type mismatch: " + key);
                        _map.remove(key);
                    } else
                        return (T) ref;
                }
            }
        }
        return null;
    }

    @Override
    public <T extends JObject> Optional<T> get(String name, Class<T> clazz) {
        cleanup();
        synchronized (_map) {
            var inMap = getFromMap(name, clazz);
            if (inMap != null) return Optional.of(inMap);
        }

        var read = jObjectRepository.readJObjectChecked(name, clazz);

        if (read.isEmpty())
            return Optional.empty();

        synchronized (_map) {
            var inMap = getFromMap(name, clazz);
            if (inMap != null) return Optional.of(inMap);
            _map.put(name, new NamedSoftReference(read.get(), _refQueue));
        }

        return Optional.of(read.get());
    }

    @Override
    public <T extends JObject> void put(T object) {
        cleanup();

        synchronized (_map) {
            var inMap = getFromMap(object.getName(), object.getClass());
            if (inMap != null && inMap != object) {
                //FIXME:
                if (object instanceof ChunkInfo) return;
                if (object instanceof ChunkData) return;
                throw new IllegalArgumentException("Trying to insert different object with same key");
            } else if (inMap == null)
                _map.put(object.getName(), new NamedSoftReference(object, _refQueue));
        }

        jObjectRepository.writeJObject(object);
    }

    @Override
    public void invalidateJObject(String name) {
        synchronized (_map) {
            _map.remove(name);
        }
    }
}
