package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;

public interface ConflictResolver {
    public enum ConflictResolutionResult {
        RESOLVED,
        FAILED
    }

    public ConflictResolutionResult
    resolve(String conflictHost, ObjectHeader conflictSource, ObjectMetaData localMeta);
}
