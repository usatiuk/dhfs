package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.RemoteObject;
import org.pcollections.PMap;

public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog) implements Op {
    public IndexUpdateOp(RemoteObject<?> object) {
        this(object.key(), object.meta().changelog());
    }
}
