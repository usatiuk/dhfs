package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.NotImplementedException;

@ApplicationScoped
public class NotImplementedConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(String conflictHost, JObject<?> conflictSource) {
        throw new NotImplementedException();
    }
}
