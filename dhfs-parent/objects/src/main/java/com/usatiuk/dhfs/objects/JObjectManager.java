package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class JObjectManager {
    @Inject
    ObjectPersistentStore objectStorage;

    @Inject
    DataLocker dataLocker;

    public class Transaction implements JObjectInterface {
        private final Map<JObjectKey, JObject> _objects = new HashMap<>();

        private JObject dataToObject(JData data) {
            return data.binder().apply(this);
        }

        @Override
        public Optional<JObject> getObject(JObjectKey key) {
            if (_objects.containsKey(key)) {
                return Optional.of(_objects.get(key));
            }

            var data = objectStorage.readObject(key).orElse(null);
            if (data == null) {
                return Optional.empty();
            }
            var ret = dataToObject(data);
            _objects.put(key, ret);
            return Optional.of(ret);
        }

        @Override
        public <T extends JObject> Optional<T> getObject(JObjectKey key, Class<T> type) {
            if (_objects.containsKey(key)) {
                var got = _objects.get(key);
                if (type.isInstance(got)) {
                    return Optional.of(type.cast(got));
                } else {
                    throw new IllegalArgumentException("Object type mismatch");
                }
            }

            var data = objectStorage.readObject(key).orElse(null);
            if (data == null) {
                return Optional.empty();
            }
            var got = dataToObject(data);
            if (type.isInstance(got)) {
                _objects.put(key, got);
                return Optional.of(type.cast(got));
            } else {
                throw new IllegalArgumentException("Object type mismatch");
            }
        }

        public void commit() {
            _objects.forEach((key, value) -> {
                var data = (TestData) value.getData();

                if (!data.isChanged()) {
                    return;
                }

                if (_objectStorage.get(key) == null) {
                    _objectStorage.put(data.copy());
                    return;
                }

                if (_objectStorage.get(key).getVersion() <= data.getVersion()) {
                    _objectStorage.put(data.copy());
                } else {
                    throw new IllegalArgumentException("Version mismatch");
                }
            });
        }
    }

    public Transaction beginTransaction() {
        return new Transaction();
    }
}
