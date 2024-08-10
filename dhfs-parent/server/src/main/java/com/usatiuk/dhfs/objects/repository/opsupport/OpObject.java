package com.usatiuk.dhfs.objects.repository.opsupport;

import java.util.UUID;

public interface OpObject {
    String getId();

    Op getPendingOpForHost(UUID host);

    void commitOpForHost(UUID host, Op op);

    void pushBootstrap(UUID host);

    void acceptExternalOp(UUID from, Op op);

    Op getPeriodicPushOp();
}
