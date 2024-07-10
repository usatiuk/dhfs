package com.usatiuk.dhfs.files.conflicts;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.NotImplementedException;

import java.util.UUID;

@ApplicationScoped
public class NotImplementedConflictResolver implements ConflictResolver {
    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {
        throw new NotImplementedException();
    }
}
