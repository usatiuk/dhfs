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

    public <T extends JObjectData> SoftJObject<T> create(Class<T> klass, String name) {
        return new SoftJObjectImpl<>(klass, name);
    }

    public <T extends JObjectData> SoftJObject<T> create(Class<T> klass, JObject<? extends T> obj) {
        return new SoftJObjectImpl<>(klass, obj);
    }

    private class SoftJObjectImpl<T extends JObjectData> implements SoftJObject<T> {
        private final Class<T> _klass;
        private final String _objName;
        private final AtomicReference<SoftReference<? extends JObject<? extends T>>> _obj;

        private SoftJObjectImpl(Class<T> klass, @NonNull String objName) {
            _klass = klass;
            _objName = objName;
            _obj = new AtomicReference<>();
        }

        private SoftJObjectImpl(Class<T> klass, JObject<? extends T> obj) {
            _klass = klass;
            _objName = obj.getMeta().getName();
            _obj = new AtomicReference<>(new SoftReference<>(obj));
        }

        @Override
        public JObject<? extends T> get() {
            while (true) {
                var have = _obj.get();
                if (have != null) {
                    var ref = have.get();
                    if (ref != null)
                        return ref;
                }
                var got = jObjectManager.get(_objName).orElse(null);
                if (got == null) return null;
                var checked = got.as(_klass);
                var next = new SoftReference<>(checked);
                if (_obj.compareAndSet(have, next))
                    return checked;
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
