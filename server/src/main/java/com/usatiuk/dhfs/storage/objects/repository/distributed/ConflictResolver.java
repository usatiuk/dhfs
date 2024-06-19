package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;

public interface ConflictResolver {
    enum ConflictResolutionResult {
        RESOLVED,
        FAILED
    }

    ConflictResolutionResult
    resolve(String conflictHost, ObjectHeader conflictSource, String localName);
}
