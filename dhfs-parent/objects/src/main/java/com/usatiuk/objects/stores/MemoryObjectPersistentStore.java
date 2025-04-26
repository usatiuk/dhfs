package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.NavigableMapKvIterator;
import com.usatiuk.objects.snapshot.Snapshot;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "memory")
public class MemoryObjectPersistentStore implements ObjectPersistentStore {
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private TreePMap<JObjectKey, ByteString> _objects = TreePMap.empty();
    private long _lastCommitId = 0;

    @Override
    public Snapshot<JObjectKey, ByteString> getSnapshot() {
        synchronized (this) {
            return new Snapshot<JObjectKey, ByteString>() {
                private final TreePMap<JObjectKey, ByteString> _objects = MemoryObjectPersistentStore.this._objects;
                private final long _lastCommitId = MemoryObjectPersistentStore.this._lastCommitId;

                @Override
                public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
                    return new NavigableMapKvIterator<>(_objects, start, key);
                }

                @Nonnull
                @Override
                public Optional<ByteString> readObject(JObjectKey name) {
                    return Optional.ofNullable(_objects.get(name));
                }

                @Override
                public long id() {
                    return _lastCommitId;
                }

                @Override
                public void close() {

                }
            };
        }
    }

    public Runnable prepareTx(TxManifestRaw names, long txId) {
        return () -> {
            synchronized (this) {
                for (var written : names.written()) {
                    _objects = _objects.plus(written.getKey(), written.getValue());
                }
                for (JObjectKey key : names.deleted()) {
                    _objects = _objects.minus(key);
                }
                assert txId > _lastCommitId;
                _lastCommitId = txId;
            }
        };
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
