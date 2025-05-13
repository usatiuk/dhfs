package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;

/**
 * Interface for handling operations.
 * @param <T> the type of operation
 */
public interface OpHandler<T extends Op> {
    /**
     * Handles the given operation.
     *
     * @param from the ID of the peer that sent the operation
     * @param op   the operation to handle
     */
    void handleOp(PeerId from, T op);
}
