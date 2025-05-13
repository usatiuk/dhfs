package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Interface for a persistent store of objects.
 * Does not have to be thread-safe! (yet), it is expected that all commits are done by the same thread.
 */
public interface ObjectPersistentStore {
    /**
     * Get a snapshot of the persistent store.
     * @return a snapshot of the persistent store
     */
    Snapshot<JObjectKey, ByteBuffer> getSnapshot();

    /**
     * Commit a transaction to the persistent store.
     * @param names the transaction manifest
     * @param txId the transaction ID
     */
    void commitTx(TxManifestRaw names, long txId);

    /**
     * Get the size of the persistent store.
     * @return the size of the persistent store
     */
    long getTotalSpace();

    /**
     * Get the free space of the persistent store.
     * @return the free space of the persistent store
     */
    long getFreeSpace();
}
