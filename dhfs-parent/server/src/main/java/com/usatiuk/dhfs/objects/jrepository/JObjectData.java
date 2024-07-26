package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.ConflictResolver;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;

public abstract class JObjectData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1;

    public abstract String getName();

    public abstract Class<? extends ConflictResolver> getConflictResolver();

    public Class<? extends JObjectData> getRefType() {
        throw new UnsupportedOperationException("This object shouldn't have refs");
    }

    public abstract Collection<String> extractRefs();

    public long estimateSize() {
        return 0;
    }
}
