package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;

public interface ConflictResolver {
    enum ConflictResolutionResult {
        RESOLVED,
        FAILED
    }

    ConflictResolutionResult
    resolve(String conflictHost, JObject<?> conflictSource);
}
