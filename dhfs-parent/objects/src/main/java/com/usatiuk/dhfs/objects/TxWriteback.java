package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.utils.VoidFn;

public interface TxWriteback {
    TxBundle createBundle();

    void commitBundle(TxBundle bundle);

    void dropBundle(TxBundle bundle);

    void fence(long bundleId);

    // Executes callback after bundle with bundleId id has been persisted
    // if it was already, runs callback on the caller thread
    void asyncFence(long bundleId, VoidFn callback);
}
