package com.usatiuk.dhfs.storage.files.conflicts;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.NotImplementedException;

import java.util.UUID;

@ApplicationScoped
public class NotImplementedConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, JObject<?> conflictSource) {
        throw new NotImplementedException();
    }
}
