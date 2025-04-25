package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;

public interface OpHandler<T extends Op> {
    void handleOp(PeerId from, T op);
}
