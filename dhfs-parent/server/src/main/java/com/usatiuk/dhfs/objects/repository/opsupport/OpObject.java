package com.usatiuk.dhfs.objects.repository.opsupport;

import java.util.List;
import java.util.UUID;

public interface OpObject {
    String getId();

    boolean hasPendingOpsForHost(UUID host);

    List<Op> getPendingOpsForHost(UUID host, int limit);

    void commitOpForHost(UUID host, Op op);

    void pushBootstrap(UUID host);

    void acceptExternalOp(UUID from, Op op);

    Op getPeriodicPushOp();

    void addToTx();
}
