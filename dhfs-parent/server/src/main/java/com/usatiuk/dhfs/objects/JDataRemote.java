package com.usatiuk.dhfs.objects;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface JDataRemote extends Serializable {
    JObjectKey key();

    default int estimateSize() {
        return 100;
    }

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}
