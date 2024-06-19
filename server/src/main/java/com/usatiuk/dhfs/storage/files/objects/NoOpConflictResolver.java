package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetaData;

public class NoOpConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(String conflictHost, ObjectHeader conflictSource, ObjectMetaData localMeta) {
        // Maybe check types?
        return ConflictResolutionResult.RESOLVED;
    }
}
