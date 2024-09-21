package com.usatiuk.dhfs.objects.jrepository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.NonNull;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class SoftJObjectFactory {
    @Inject
    JObjectManager jObjectManager;

    public <T extends JObjectData> SoftJObject<T> create(String name) {
        return new SoftJObjectImpl<>(name);
    }

    public <T extends JObjectData> SoftJObject<T> create(JObject<T> obj) {
        return new SoftJObjectImpl<>(obj);
    }

    private class SoftJObjectImpl<T extends JObjectData> implements SoftJObject<T> {
        private final String _objName;
        private final AtomicReference<SoftReference<JObject<T>>> _obj;

        private SoftJObjectImpl(@NonNull String objName) {
            _objName = objName;
            _obj = new AtomicReference<>();
        }

        private SoftJObjectImpl(JObject<T> obj) {
            _objName = obj.getMeta().getName();
            _obj = new AtomicReference<>(new SoftReference<>(obj));
        }

        @Override
        public JObject<T> get() {
            while (true) {
                var have = _obj.get();
                if (have != null) {
                    var ref = have.get();
                    if (ref != null)
                        return ref;
                }
                var got = (JObject<T>) jObjectManager.get(_objName).orElse(null);
                if (got == null) return null;
                var next = new SoftReference<>(got);
                _obj.compareAndSet(have, next);
            }
        }

        @Override
        public String getName() {
            return _objName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SoftJObjectImpl<?> that = (SoftJObjectImpl<?>) o;
            return _objName.equals(that._objName);
        }

        @Override
        public int hashCode() {
            return _objName.hashCode();
        }
    }
}
