package com.usatiuk.dhfs.objects;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.RemoteObjectP;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

@ProtoMirror(RemoteObjectP.class)
public interface JDataRemote extends Serializable {
    JObjectKey key();

    default int estimateSize() {
        return 100;
    }

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}
