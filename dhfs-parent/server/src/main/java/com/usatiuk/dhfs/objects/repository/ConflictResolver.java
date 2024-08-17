package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;

import java.util.UUID;

public interface ConflictResolver {
    void resolve(UUID conflictHost, ObjectHeader conflictHeader, JObjectData conflictData, JObjectManager.JObject<?> conflictSource);
}
