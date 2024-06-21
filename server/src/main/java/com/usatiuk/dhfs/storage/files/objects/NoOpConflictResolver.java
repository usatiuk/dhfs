package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class NoOpConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(String conflictHost, ObjectHeader conflictSource, String localName) {
        // Maybe check types?
        return ConflictResolutionResult.RESOLVED;
    }
}
