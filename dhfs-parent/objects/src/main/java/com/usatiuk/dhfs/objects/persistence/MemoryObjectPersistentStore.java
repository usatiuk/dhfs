package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JObjectKey;
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
    public void commitTx(TxManifestRaw names) {
        synchronized (this) {
            for (var written : names.written()) {
                _objects.put(written.getKey(), written.getValue());
            }
            for (JObjectKey key : names.deleted()) {
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
