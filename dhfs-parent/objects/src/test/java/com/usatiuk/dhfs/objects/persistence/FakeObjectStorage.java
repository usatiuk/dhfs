package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.test.objs.TestData;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FakeObjectStorage implements ObjectPersistentStore {
    private final Map<JObjectKey, TestData> _objects = new HashMap<>();
    private final Map<JObjectKey, TestData> _pending = new HashMap<>();

    @Nonnull
    @Override
    public Collection<JObjectKey> findAllObjects() {
        synchronized (this) {
            return _objects.keySet();
        }
    }

    @Nonnull
    @Override
    public Optional<JData> readObject(JObjectKey name) {
        synchronized (this) {
            return Optional.ofNullable(_objects.get(name));
        }
    }

    @Override
    public void writeObject(JObjectKey name, JData object) {
        synchronized (this) {
            _pending.put(name, (TestData) object);
        }
    }

    @Override
    public void commitTx(TxManifest names) {
        synchronized (this) {
            for (JObjectKey key : names.getWritten()) {
                _objects.put(key, _pending.get(key));
            }
            for (JObjectKey key : names.getDeleted()) {
                _objects.remove(key);
            }
        }
    }

    @Override
    public void deleteObjectDirect(JObjectKey name) {
        synchronized (this) {
            _objects.remove(name);
        }
    }

    @Override
    public long getTotalSpace() {
        return 0;
    }

    @Override
    public long getFreeSpace() {
        return 0;
    }

    @Override
    public long getUsableSpace() {
        return 0;
    }
}
