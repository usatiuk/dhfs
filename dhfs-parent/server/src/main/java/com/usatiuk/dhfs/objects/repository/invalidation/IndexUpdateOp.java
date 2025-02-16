package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.RemoteObjectMeta;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.List;

public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog) implements Op {
    public IndexUpdateOp(RemoteObjectMeta object) {
        this(object.key(), object.changelog());
    }

    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return List.of(key);
    }
}
