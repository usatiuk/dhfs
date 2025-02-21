package com.usatiuk.dhfs.objects;

import java.util.Collection;
import java.util.Optional;

public interface TxWriteback {
    TxBundle createBundle();

    void commitBundle(TxBundle bundle);

    void dropBundle(TxBundle bundle);

    void fence(long bundleId);

    Optional<PendingWriteEntry> getPendingWrite(JObjectKey key);
    Collection<PendingWriteEntry> getPendingWrites();

    // Executes callback after bundle with bundleId id has been persisted
    // if it was already, runs callback on the caller thread
    void asyncFence(long bundleId, Runnable callback);

    interface PendingWriteEntry {
        long bundleId();
    }

    record PendingWrite(JDataVersionedWrapper<?> data, long bundleId) implements PendingWriteEntry {
    }

    record PendingDelete(JObjectKey key, long bundleId) implements PendingWriteEntry {
    }
}
