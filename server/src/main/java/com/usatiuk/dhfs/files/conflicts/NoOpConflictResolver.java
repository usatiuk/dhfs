package com.usatiuk.dhfs.files.conflicts;

import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@ApplicationScoped
public class NoOpConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {
        // Maybe check types?
        return ConflictResolutionResult.RESOLVED;
    }
}
