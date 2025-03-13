package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.CloseableKvIterator;
import com.usatiuk.dhfs.objects.IterProdFn;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

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

    Snapshot<JObjectKey, ByteString> getSnapshot();

    /**
     * @param commitLocked - a function that will be called with a Runnable that will commit the transaction
     *                     the changes in the store will be visible to new transactions only after the runnable is called
     */
    void commitTx(TxManifestRaw names, long txId, Consumer<Runnable> commitLocked);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();

    long getLastCommitId();
}
