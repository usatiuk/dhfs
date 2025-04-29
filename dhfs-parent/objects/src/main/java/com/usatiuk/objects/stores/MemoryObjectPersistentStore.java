package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "memory")
public class MemoryObjectPersistentStore implements ObjectPersistentStore {
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    private TreePMap<JObjectKey, ByteString> _objects = TreePMap.empty();
    private long _lastCommitId = 0;

    @Override
    public Snapshot<JObjectKey, ByteBuffer> getSnapshot() {
        synchronized (this) {
            return new Snapshot<JObjectKey, ByteBuffer>() {
                private final TreePMap<JObjectKey, ByteString> _objects = MemoryObjectPersistentStore.this._objects;
                private final long _lastCommitId = MemoryObjectPersistentStore.this._lastCommitId;

                @Override
                public List<CloseableKvIterator<JObjectKey, MaybeTombstone<ByteBuffer>>> getIterator(IteratorStart start, JObjectKey key) {
                    return List.of(new MappingKvIterator<>(new NavigableMapKvIterator<>(_objects, start, key), s -> new DataWrapper<>(s.asReadOnlyByteBuffer())));
                }

                @Nonnull
                @Override
                public Optional<ByteBuffer> readObject(JObjectKey name) {
                    return Optional.ofNullable(_objects.get(name)).map(ByteString::asReadOnlyByteBuffer);
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
