package com.usatiuk.dhfs.objects.jrepository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class SoftJObjectFactory {
    @Inject
    JObjectManager jObjectManager;

    public <T extends JObjectData> SoftJObject<T> create(String name) {
        return new SoftJObjectImpl<>(name);
    }

    private class SoftJObjectImpl<T extends JObjectData> implements SoftJObject<T> {
        private final String _objName;
        private final AtomicReference<SoftReference<JObject<T>>> _obj = new AtomicReference<>(null);

        private SoftJObjectImpl(String objName) {_objName = objName;}

        @Override
        public JObject<T> get() {
            while (true) {
                var have = _obj.get();
                if (have != null) {
                    var ref = have.get();
                    if (ref != null)
                        return ref;
                }
                var next = new SoftReference<>((JObject<T>) jObjectManager.get(_objName).get());
                _obj.compareAndSet(have, next);
            }
        }
    }
}
