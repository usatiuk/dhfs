package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.ConflictResolver;

import java.io.Serializable;
import java.util.Collection;

public abstract class JObjectData implements Serializable {
    public abstract String getName();

    public abstract Class<? extends ConflictResolver> getConflictResolver();

    public boolean assumeUnique() {
        return false;
    }

    public Class<? extends JObjectData> getRefType() {
        throw new UnsupportedOperationException("This object shouldn't have refs");
    }

    public boolean pushResolution() {
        return false;
    }

    public abstract Collection<String> extractRefs();

    public long estimateSize() {
        return 0;
    }
}
