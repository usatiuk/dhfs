package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.CloseableKvIterator;
import com.usatiuk.dhfs.objects.JObjectKey;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

// Persistent storage of objects
// All changes are written as sequential transactions
public interface ObjectPersistentStore {
    @Nonnull
    Collection<JObjectKey> findAllObjects();

    @Nonnull
    Optional<ByteString> readObject(JObjectKey name);

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    CloseableKvIterator<JObjectKey, ByteString> getIterator(IteratorStart start, JObjectKey key);

    default CloseableKvIterator<JObjectKey, ByteString> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }

    void commitTx(TxManifestRaw names);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
