package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class NoOpConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(String conflictHost, JObject<?> conflictSource) {
        // Maybe check types?
        return ConflictResolutionResult.RESOLVED;
    }
}
