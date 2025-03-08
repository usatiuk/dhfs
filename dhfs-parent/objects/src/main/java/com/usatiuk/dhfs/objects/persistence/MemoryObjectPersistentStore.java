package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.CloseableKvIterator;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.NavigableMapKvIterator;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "memory")
public class MemoryObjectPersistentStore implements ObjectPersistentStore {
    private final ConcurrentSkipListMap<JObjectKey, ByteString> _objects = new ConcurrentSkipListMap<>();
    private long _lastCommitId = 0;
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

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
    public CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key) {
        return new NavigableMapKvIterator<>(_objects, start, key);
    }

    @Override
    public void commitTx(TxManifestRaw names, long txId, Consumer<Runnable> commitLocked) {
        synchronized (this) {
            for (var written : names.written()) {
                _objects.put(written.getKey(), written.getValue());
            }
            for (JObjectKey key : names.deleted()) {
                _objects.remove(key);
            }
            commitLocked.accept(() -> {
                _lock.writeLock().lock();
                try {
                    assert txId > _lastCommitId;
                    _lastCommitId = txId;
                } finally {
                    _lock.writeLock().unlock();
                }
            });
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

    @Override
    public long getLastCommitId() {
        _lock.readLock().lock();
        try {
            return _lastCommitId;
        } finally {
            _lock.readLock().unlock();
        }
    }
}
