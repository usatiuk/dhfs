package com.usatiuk.dhfs.objects.repository.invalidation;

import java.util.UUID;

public interface IncomingOpListener {
    void accept(UUID incomingPeer, Op op);
}
