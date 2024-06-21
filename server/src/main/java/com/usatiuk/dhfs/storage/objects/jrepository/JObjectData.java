package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;

import java.io.Serializable;

public abstract class JObjectData implements Serializable {
    public abstract String getName();
    public abstract Class<? extends ConflictResolver> getConflictResolver();

    public boolean assumeUnique() {
        return false;
    }
}
