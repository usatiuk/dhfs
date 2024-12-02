package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JObjectKey;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "memory")
public class MemoryObjectPersistentStore implements ObjectPersistentStore {
    private final Map<JObjectKey, ByteString> _objects = new HashMap<>();
    private final Map<JObjectKey, ByteString> _pending = new HashMap<>();

    @Nonnull
    @Override
    public Collection<JObjectKey> findAllObjects() {
        synchronized (this) {
            return _objects.keySet();
        }
    }

    @Nonnull
    @Override
    public Optional<ByteString> readObject(JObjectKey name) {
        synchronized (this) {
            return Optional.ofNullable(_objects.get(name));
        }
    }

    @Override
    public void writeObject(JObjectKey name, ByteString object) {
        synchronized (this) {
            _pending.put(name, object);
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
