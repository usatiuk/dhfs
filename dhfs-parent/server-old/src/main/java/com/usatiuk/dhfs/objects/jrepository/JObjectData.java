package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;

import java.util.Collection;
import java.util.List;

@ProtoMirror(JObjectDataP.class)
public abstract class JObjectData {
    public abstract String getName();

    public Class<? extends ConflictResolver> getConflictResolver() {
        throw new UnsupportedOperationException();
    }

    public Class<? extends JObjectData> getRefType() {
        throw new UnsupportedOperationException("This object shouldn't have refs");
    }

    public Collection<String> extractRefs() {
        return List.of();
    }

    public int estimateSize() {
        return 0;
    }
}
