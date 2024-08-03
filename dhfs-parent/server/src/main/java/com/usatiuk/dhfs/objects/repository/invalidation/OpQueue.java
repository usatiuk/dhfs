package com.usatiuk.dhfs.objects.repository.invalidation;

import java.util.Collection;
import java.util.UUID;

public interface OpQueue {
    Object getForHost(UUID host);
}
