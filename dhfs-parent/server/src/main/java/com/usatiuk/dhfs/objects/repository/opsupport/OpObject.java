package com.usatiuk.dhfs.objects.repository.opsupport;

import java.util.List;
import java.util.UUID;

public interface OpObject<OpT extends Op> {
    String getId();

    OpT getPendingOpForHost(UUID host);

    void commitOpForHost(UUID host, OpT op);

    List<OpT> getBootstrap();

    void acceptExternalOp(UUID from, OpT op);
}
