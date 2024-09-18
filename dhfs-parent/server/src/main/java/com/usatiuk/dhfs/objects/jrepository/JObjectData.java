package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;

@ProtoMirror(JObjectDataP.class)
public abstract class JObjectData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1;

    public abstract String getName();

    public abstract Class<? extends ConflictResolver> getConflictResolver();

    public Class<? extends JObjectData> getRefType() {
        throw new UnsupportedOperationException("This object shouldn't have refs");
    }

    public abstract Collection<String> extractRefs();

    public int estimateSize() {
        return 0;
    }
}
