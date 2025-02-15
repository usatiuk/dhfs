package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.RemoteObjectMeta;
import org.pcollections.PMap;

public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog) implements Op {
    public IndexUpdateOp(RemoteObjectMeta object) {
        this(object.key(), object.changelog());
    }
}
