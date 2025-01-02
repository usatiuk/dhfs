package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.utils.VoidFn;

import java.util.Optional;

public interface TxWriteback {
    TxBundle createBundle();

    void commitBundle(TxBundle bundle);

    void dropBundle(TxBundle bundle);

    void fence(long bundleId);

    interface PendingWriteEntry {
        long bundleId();
    }

    record PendingWrite(JDataVersionedWrapper<?> data, long bundleId) implements PendingWriteEntry {
    }

    record PendingDelete(JObjectKey key, long bundleId) implements PendingWriteEntry {
    }

    Optional<PendingWriteEntry> getPendingWrite(JObjectKey key);

    // Executes callback after bundle with bundleId id has been persisted
    // if it was already, runs callback on the caller thread
    void asyncFence(long bundleId, VoidFn callback);
}
