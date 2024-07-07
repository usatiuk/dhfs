package com.usatiuk.dhfs.storage.objects.repository;

import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;

import java.util.UUID;

public interface ConflictResolver {
    enum ConflictResolutionResult {
        RESOLVED,
        FAILED
    }

    ConflictResolutionResult
    resolve(UUID conflictHost, ObjectHeader conflictHeader, JObjectData conflictData, JObject<?> conflictSource);
}
