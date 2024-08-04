package com.usatiuk.dhfs.objects.repository.invalidation;

import java.util.UUID;

public interface OpQueue {
    Op getForHost(UUID host);

    String getId();

    void commitOneForHost(UUID host, Op op);
}
