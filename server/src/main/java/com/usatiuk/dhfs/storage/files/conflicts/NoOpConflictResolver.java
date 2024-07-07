package com.usatiuk.dhfs.storage.files.conflicts;

import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.ConflictResolver;
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
